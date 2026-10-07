#!/usr/bin/env python3
"""Off-market business scout: TX / NAICS 238220 / PPP >= $150K.

Standard library only. See SCOUT.md for the runbook.

    python3 scout.py all        # fetch (cached) + build + verify
    python3 scout.py fetch      # download sources, write data/download_log.csv
    python3 scout.py build      # filter, group, match, write tracker + page
    python3 scout.py verify     # integrity checks on the current outputs
"""
import csv
import datetime as dt
import hashlib
import html
import io
import json
import os
import re
import shutil
import sys
import urllib.request
import zipfile
from collections import Counter, defaultdict

ROOT = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(ROOT, "data")
RAW = os.path.join(DATA, "raw")
CFG = json.load(open(os.path.join(ROOT, "scout_config.json")))
TRACKER = os.path.join(ROOT, "scout-tracker.csv")
RESULTS = os.path.join(ROOT, "results.html")

P = {
    "download_log": os.path.join(DATA, "download_log.csv"),
    "manifest": os.path.join(DATA, "source_manifest.json"),
    "sba_extract": os.path.join(DATA, "extracts", "sba_ppp_tx_238220_150k_raw_rows.csv"),
    "loans": os.path.join(DATA, "loans.csv"),
    "registry": os.path.join(DATA, "business_registry.csv"),
    "ledger": os.path.join(DATA, "source_ledger.csv"),
    "tx_candidates": os.path.join(DATA, "tx_franchise_candidates.csv"),
    "plans": os.path.join(DATA, "pension_plans.csv"),
    "review": os.path.join(DATA, "review_queue.csv"),
    "unavailable": os.path.join(DATA, "unavailable_sources.csv"),
    "runs": os.path.join(DATA, "run_history.csv"),
    "coverage": os.path.join(DATA, "coverage_report.json"),
    "verification": os.path.join(DATA, "verification.json"),
}

csv.field_size_limit(1 << 30)
UA = {"User-Agent": "off-market-scout/1.0 (+SCOUT.md)"}
NOW = dt.datetime.now(dt.timezone.utc)
RUN_ID = NOW.strftime("%Y%m%dT%H%M%SZ")
TODAY = NOW.date()


# --------------------------------------------------------------------------
# small IO helpers
# --------------------------------------------------------------------------
def ensure_dirs():
    for d in (DATA, RAW, os.path.dirname(P["sba_extract"])):
        os.makedirs(d, exist_ok=True)


def read_csv(path):
    if not os.path.exists(path):
        return [], []
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        rows = list(r)
        return (r.fieldnames or []), rows


def write_csv(path, fields, rows):
    tmp = path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: ("" if r.get(k) is None else r.get(k)) for k in fields})
    os.replace(tmp, path)


def append_csv(path, fields, rows):
    new = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if new:
            w.writeheader()
        for r in rows:
            w.writerow(r)


def log(msg):
    print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


class LineDecoder:
    """Decode a byte stream line by line: strict UTF-8 first, cp1252 then latin-1 per line.

    Keeps exact counts so the manifest can state the encoding that was actually seen."""

    def __init__(self):
        self.lines = 0
        self.utf8_non_ascii = 0
        self.cp1252 = 0
        self.latin1 = 0
        self.bom = False

    def decode(self, b):
        self.lines += 1
        if self.lines == 1 and b.startswith(b"\xef\xbb\xbf"):
            self.bom = True
            b = b[3:]
        try:
            s = b.decode("utf-8")
            if not s.isascii():
                self.utf8_non_ascii += 1
            return s
        except UnicodeDecodeError:
            pass
        try:
            s = b.decode("cp1252")
            self.cp1252 += 1
            return s
        except UnicodeDecodeError:
            self.latin1 += 1
            return b.decode("latin-1")

    def summary(self):
        if self.cp1252 == 0 and self.latin1 == 0:
            enc = "utf-8" if self.utf8_non_ascii else "ascii (utf-8 compatible)"
        elif self.utf8_non_ascii == 0:
            enc = "cp1252"
        else:
            enc = "mixed: utf-8 with cp1252/latin-1 lines"
        return {
            "encoding_detected": enc + (" with BOM" if self.bom else ""),
            "physical_lines": self.lines,
            "lines_utf8_non_ascii": self.utf8_non_ascii,
            "lines_decoded_cp1252_fallback": self.cp1252,
            "lines_decoded_latin1_fallback": self.latin1,
        }


def decoded_lines(binary_stream, decoder):
    for b in binary_stream:
        yield decoder.decode(b)


# --------------------------------------------------------------------------
# fetch
# --------------------------------------------------------------------------
LOG_FIELDS = ["run_id", "source_id", "url", "final_url", "retrieved_at_utc", "http_status",
              "etag", "last_modified", "bytes", "sha256", "raw_path", "action", "note"]


def http_get(url, timeout=120):
    req = urllib.request.Request(url, headers=UA)
    return urllib.request.urlopen(req, timeout=timeout)


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def head_info(url):
    try:
        req = urllib.request.Request(url, headers=UA, method="HEAD")
        with urllib.request.urlopen(req, timeout=60) as r:
            return {"status": r.status, "etag": r.headers.get("ETag", ""),
                    "last_modified": r.headers.get("Last-Modified", ""),
                    "length": r.headers.get("Content-Length", ""), "final_url": r.geturl()}
    except Exception as e:  # noqa: BLE001
        return {"status": "error", "error": str(e)}


def download(source_id, url, dest, force=False):
    """Stream url -> dest while hashing. Reuse the cached file when ETag/Last-Modified match."""
    meta_path = dest + ".meta.json"
    head = head_info(url)
    row = {"run_id": RUN_ID, "source_id": source_id, "url": url,
           "retrieved_at_utc": NOW.isoformat(timespec="seconds"), "raw_path": os.path.relpath(dest, ROOT)}
    if os.path.exists(dest) and os.path.exists(meta_path) and not force:
        meta = json.load(open(meta_path))
        same = head.get("status") == 200 and (
            (head.get("etag") and head["etag"] == meta.get("etag")) or
            (head.get("last_modified") and head["last_modified"] == meta.get("last_modified")))
        if same or head.get("status") != 200:
            row.update({"final_url": meta.get("final_url", url), "http_status": head.get("status"),
                        "etag": meta.get("etag"), "last_modified": meta.get("last_modified"),
                        "bytes": meta["bytes"], "sha256": meta["sha256"],
                        "action": "cached" if same else "cached_remote_unreachable",
                        "note": f"originally retrieved {meta['retrieved_at_utc']}"})
            append_csv(P["download_log"], LOG_FIELDS, [row])
            log(f"{source_id}: using cached {os.path.basename(dest)} ({row['action']})")
            return meta
    log(f"{source_id}: downloading {url}")
    h = hashlib.sha256()
    n = 0
    part = dest + ".part"
    with http_get(url, timeout=300) as r, open(part, "wb") as f:
        status = r.status
        final_url = r.geturl()
        etag = r.headers.get("ETag", "") or head.get("etag", "")
        lm = r.headers.get("Last-Modified", "") or head.get("last_modified", "")
        while True:
            chunk = r.read(1 << 20)
            if not chunk:
                break
            h.update(chunk)
            f.write(chunk)
            n += len(chunk)
    os.replace(part, dest)
    meta = {"source_id": source_id, "url": url, "final_url": final_url, "http_status": status,
            "etag": etag, "last_modified": lm, "bytes": n, "sha256": h.hexdigest(),
            "retrieved_at_utc": NOW.isoformat(timespec="seconds")}
    json.dump(meta, open(meta_path, "w"), indent=1)
    row.update({"final_url": final_url, "http_status": status, "etag": etag, "last_modified": lm,
                "bytes": n, "sha256": meta["sha256"], "action": "downloaded", "note": ""})
    append_csv(P["download_log"], LOG_FIELDS, [row])
    log(f"{source_id}: {n:,} bytes sha256={meta['sha256'][:12]}")
    return meta


def discover_sba_url():
    s = CFG["sources"]["SBA_PPP_150K_PLUS"]
    try:
        page = http_get(s["dataset_page"]).read().decode("utf-8", "replace")
        urls = sorted(set(re.findall(r'https?://[^"\']*' + s["url_pattern"], page)))
        if urls:
            return urls[-1], "discovered on dataset page"
    except Exception as e:  # noqa: BLE001
        return s["fallback_url"], f"dataset page unreachable ({e}); using configured URL"
    return s["fallback_url"], "pattern not found on dataset page; using configured URL"


def discover_dol_years():
    s = CFG["sources"]["DOL_FORM_5500"]
    try:
        page = http_get(s["dataset_page"]).read().decode("utf-8", "replace")
        years = sorted({int(y) for y in re.findall(r"F_5500_(\d{4})_Latest\.zip", page)})
        sf_years = {int(y) for y in re.findall(r"F_5500_SF_(\d{4})_Latest\.zip", page)}
        years = [y for y in years if y in sf_years]
        if years:
            return years[-2:]
    except Exception:  # noqa: BLE001
        pass
    y = TODAY.year - 1
    return [y - 1, y]


def cmd_fetch(force=False):
    ensure_dirs()
    manifest = load_manifest()
    fetched = {}
    sba_url, how = discover_sba_url()
    s = CFG["sources"]["SBA_PPP_150K_PLUS"]
    fetched["SBA_PPP_150K_PLUS"] = safe_download("SBA_PPP_150K_PLUS", sba_url, os.path.join(RAW, s["raw_file"]), force)
    if fetched["SBA_PPP_150K_PLUS"]:
        fetched["SBA_PPP_150K_PLUS"]["discovery"] = how
    t = CFG["sources"]["TX_FRANCHISE_9CIR_EFMM"]
    tx_meta = {}
    try:
        m = json.load(http_get(t["metadata_url"]))
        tx_meta = {"rows_updated_at": dt.datetime.fromtimestamp(m.get("rowsUpdatedAt", 0), dt.timezone.utc).isoformat(),
                   "attribution": m.get("attribution")}
    except Exception as e:  # noqa: BLE001
        tx_meta = {"error": str(e)}
    fetched["TX_FRANCHISE_9CIR_EFMM"] = safe_download("TX_FRANCHISE_9CIR_EFMM", t["url"], os.path.join(RAW, t["raw_file"]), force)
    if fetched["TX_FRANCHISE_9CIR_EFMM"]:
        fetched["TX_FRANCHISE_9CIR_EFMM"]["dataset_metadata"] = tx_meta
    years = discover_dol_years()
    for sid in ("DOL_FORM_5500", "DOL_FORM_5500_SF"):
        d = CFG["sources"][sid]
        for y in years:
            key = f"{sid}_{y}"
            fetched[key] = safe_download(key, d["url_template"].format(year=y),
                                         os.path.join(RAW, d["raw_file_template"].format(year=y)), force)
            if fetched[key]:
                fetched[key]["plan_year"] = y
    manifest["fetch"] = {"run_id": RUN_ID, "dol_plan_years": years, "files": fetched}
    save_manifest(manifest)
    return manifest


def safe_download(source_id, url, dest, force):
    try:
        return download(source_id, url, dest, force)
    except Exception as e:  # noqa: BLE001
        append_csv(P["download_log"], LOG_FIELDS, [{
            "run_id": RUN_ID, "source_id": source_id, "url": url,
            "retrieved_at_utc": NOW.isoformat(timespec="seconds"), "action": "failed", "note": str(e)}])
        log_unavailable(source_id, source_id, url, f"download failed: {e}")
        log(f"{source_id}: FAILED {e}")
        return None


def load_manifest():
    return json.load(open(P["manifest"])) if os.path.exists(P["manifest"]) else {}


def save_manifest(m):
    json.dump(m, open(P["manifest"], "w"), indent=1, sort_keys=True)


UNAVAIL_FIELDS = ["source_id", "name", "checked_url", "reason", "first_logged_run", "first_logged_utc"]


def log_unavailable(source_id, name, url, reason):
    """Each unavailable source is logged exactly once (keyed by source_id)."""
    _, rows = read_csv(P["unavailable"])
    if any(r["source_id"] == source_id for r in rows):
        return
    append_csv(P["unavailable"], UNAVAIL_FIELDS, [{
        "source_id": source_id, "name": name, "checked_url": url, "reason": reason,
        "first_logged_run": RUN_ID, "first_logged_utc": NOW.isoformat(timespec="seconds")}])


# --------------------------------------------------------------------------
# normalisation
# --------------------------------------------------------------------------
ENTITY_SUFFIX = {"INC", "INCORPORATED", "LLC", "LLP", "LP", "LTD", "LIMITED", "CO", "COMPANY", "CORP",
                 "CORPORATION", "PLLC", "PC", "PA", "LC", "PLC", "LLLP", "COMPANIES"}
DBA_RE = re.compile(r"\s*(?:\bD\s*/\s*B\s*/\s*A\b|\bD\.B\.A\.?|\bDBA\b|\bD B A\b|\bA/K/A\b|\bAKA\b|\bT/A\b)\s*", re.I)
STREET_ABBR = {
    "STREET": "ST", "AVENUE": "AVE", "AV": "AVE", "ROAD": "RD", "DRIVE": "DR", "BOULEVARD": "BLVD", "LANE": "LN",
    "HIGHWAY": "HWY", "PARKWAY": "PKWY", "COURT": "CT", "CIRCLE": "CIR", "PLACE": "PL", "TRAIL": "TRL",
    "FREEWAY": "FWY", "EXPRESSWAY": "EXPY", "TERRACE": "TER", "SQUARE": "SQ", "CROSSING": "XING",
    "NORTH": "N", "SOUTH": "S", "EAST": "E", "WEST": "W", "NORTHEAST": "NE", "NORTHWEST": "NW",
    "SOUTHEAST": "SE", "SOUTHWEST": "SW", "FARM": "FM", "MARKET": "", "TO": "", "LOOP": "LOOP",
    "POST": "PO", "OFFICE": "", "P": "P", "O": "O", "BOX": "BOX", "INTERSTATE": "IH", "I": "IH",
    "STATE": "ST", "COUNTY": "CO", "CR": "CO RD", "SH": "ST HWY", "US": "US",
}
UNIT_RE = re.compile(r"\s(?:STE|SUITE|UNIT|APT|BLDG|BUILDING|RM|ROOM|FL|FLOOR|SPC|SPACE|LOT|DEPT|#)\b.*$")


def norm_text(s):
    s = (s or "").upper().replace("&", " AND ").replace("+", " AND ")
    s = re.sub(r"[^A-Z0-9 ]+", " ", s.replace("'", ""))
    toks = s.split()
    out, buf = [], ""
    for t in toks:  # join runs of single letters: "L L C" -> "LLC", "A B C" -> "ABC"
        if len(t) == 1 and t.isalpha():
            buf += t
            continue
        if buf:
            out.append(buf)
            buf = ""
        out.append(t)
    if buf:
        out.append(buf)
    return " ".join(out)


def split_dba(raw):
    parts = DBA_RE.split(raw or "")
    parts = [p.strip(" ,;-") for p in parts if p and p.strip(" ,;-")]
    if not parts:
        return "", []
    return parts[0], parts[1:]


def core_name(name):
    toks = norm_text(name).split()
    if toks and toks[0] == "THE":
        toks = toks[1:]
    while len(toks) > 1 and toks[-1] in ENTITY_SUFFIX:
        toks = toks[:-1]
    return " ".join(toks)


ENTITY_CLASS = {"INC": {"corp"}, "INCORPORATED": {"corp"}, "CORP": {"corp"}, "CORPORATION": {"corp"}, "CO": {"corp"},
                "COMPANY": {"corp"}, "COMPANIES": {"corp"}, "PC": {"corp"}, "PA": {"corp"}, "LLC": {"llc"}, "LC": {"llc"},
                "PLLC": {"llc"}, "LP": {"partnership"}, "LLP": {"partnership"}, "LLLP": {"partnership"}, "PLC": {"corp"},
                "LTD": {"corp", "partnership"}, "LIMITED": {"corp", "partnership"}}


def suffix_of(name):
    """Entity class of the last legal suffix token ('' when the name carries none)."""
    toks = norm_text(name).split()
    if len(toks) > 1 and toks[-1] in ENTITY_SUFFIX:
        return "/".join(sorted(ENTITY_CLASS[toks[-1]]))
    return ""


def suffix_compatible(a, b):
    return bool(set(a.split("/")) & set(b.split("/")))


def norm_street(addr):
    s = " " + norm_text((addr or "").replace("#", " # ")) + " "
    s = UNIT_RE.sub("", s.upper() + " ").strip()
    toks = []
    for t in s.split():
        t = STREET_ABBR.get(t, t)
        if t:
            toks.extend(t.split())
    s = " ".join(toks)
    s = re.sub(r"\bP ?O BOX\b", "PO BOX", s)
    s = re.sub(r"\b(\d+)(ST|ND|RD|TH)\b", r"\1", s)
    return s


def zip5(z):
    d = re.sub(r"\D", "", z or "")
    return d[:5] if len(d) >= 5 else ""


def house_key(street):
    toks = street.split()
    if not toks:
        return ""
    if toks[0] == "PO":
        return " ".join(toks[:3])
    return " ".join(toks[:2]) if toks[0][0].isdigit() else ""


def tokens_sim(a, b):
    ta, tb = set(a.split()), set(b.split())
    return len(ta & tb) / len(ta | tb) if ta and tb else 0.0


def money(x):
    try:
        return float(str(x).replace(",", "").replace("$", "") or 0)
    except ValueError:
        return 0.0


def parse_date(s):
    s = (s or "").strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%Y%m%d", "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            return dt.datetime.strptime(s[:26] if "T" in s else s, fmt).date()
        except ValueError:
            continue
    return None


def years_between(d0, d1):
    y = d1.year - d0.year - ((d1.month, d1.day) < (d0.month, d0.day))
    return y


# --------------------------------------------------------------------------
# stage 1: SBA PPP stream filter
# --------------------------------------------------------------------------
def stage_sba(manifest):
    f = (manifest.get("fetch", {}).get("files") or {}).get("SBA_PPP_150K_PLUS")
    path = os.path.join(RAW, CFG["sources"]["SBA_PPP_150K_PLUS"]["raw_file"])
    if not os.path.exists(path):
        raise SystemExit("SBA CSV missing; run `python3 scout.py fetch` (the SBA file is required).")
    tgt = CFG["target"]
    dec = LineDecoder()
    c = Counter()
    loans, dup_numbers = {}, []
    near_miss = []
    with open(path, "rb") as fb:
        rdr = csv.reader(decoded_lines(fb, dec))
        header = next(rdr)
        ix = {h: i for i, h in enumerate(header)}
        out = open(P["sba_extract"], "w", newline="", encoding="utf-8")
        w = csv.writer(out)
        w.writerow(header)
        for row in rdr:
            c["source_data_rows"] += 1
            if len(row) != len(header):
                c["rows_wrong_field_count"] += 1
                continue
            st = row[ix["BorrowerState"]].strip().upper()
            naics = row[ix["NAICSCode"]].strip()
            amt = money(row[ix["CurrentApprovalAmount"]])
            if st == tgt["borrower_state"]:
                c["rows_borrower_state_tx"] += 1
                if naics == tgt["naics_code"]:
                    c["rows_tx_naics_238220"] += 1
                    if amt >= tgt["min_current_approval_amount"]:
                        c["rows_tx_naics_amount_ge_150k"] += 1
                        rec = dict(zip(header, row))
                        ln = rec["LoanNumber"].strip()
                        if ln in loans:
                            dup_numbers.append(ln)
                            continue
                        loans[ln] = rec
                        w.writerow(row)
                    else:
                        c["rows_tx_naics_amount_lt_150k"] += 1
            elif not st and naics == tgt["naics_code"] and row[ix["ProjectState"]].strip().upper() == tgt["borrower_state"] \
                    and amt >= tgt["min_current_approval_amount"]:
                near_miss.append(row[ix["LoanNumber"]])
        out.close()
    stats = dict(c)
    stats.update(dec.summary())
    stats["header_columns"] = len(header)
    stats["duplicate_loan_numbers_in_matches"] = len(dup_numbers)
    stats["matching_unique_loans"] = len(loans)
    stats["excluded_blank_borrowerstate_but_projectstate_tx"] = len(near_miss)
    stats["excluded_blank_borrowerstate_loan_numbers"] = near_miss[:200]
    stats["sha256"] = (f or {}).get("sha256") or sha256_file(path)
    stats["bytes"] = os.path.getsize(path)
    stats["source_url"] = (f or {}).get("url", "")
    m = re.search(r"(\d{6})\.csv", stats["source_url"])
    stats["data_as_of"] = dt.datetime.strptime(m.group(1), "%y%m%d").date().isoformat() if m else ""
    stats["extract_sha256"] = sha256_file(P["sba_extract"])
    log(f"SBA: {c['source_data_rows']:,} data rows, {len(loans)} matching loans, encoding={stats['encoding_detected']}")
    return loans, stats


# --------------------------------------------------------------------------
# stage 2: group loans into businesses, assign permanent IDs
# --------------------------------------------------------------------------
class DSU:
    def __init__(self):
        self.p = {}

    def find(self, x):
        self.p.setdefault(x, x)
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]
            x = self.p[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.p[max(ra, rb)] = min(ra, rb)


def loan_identity(rec):
    legal, dbas = split_dba(rec["BorrowerName"])
    street = norm_street(rec["BorrowerAddress"])
    return {
        "legal_raw": legal, "dba_raw": dbas, "core": core_name(legal), "full": norm_text(legal),
        "dba_cores": [core_name(d) for d in dbas], "street": street, "house": house_key(street),
        "zip5": zip5(rec["BorrowerZip"]), "city": norm_text(rec["BorrowerCity"]),
    }


def group_loans(loans):
    dsu = DSU()
    ids = {ln: loan_identity(r) for ln, r in loans.items()}
    link_reason = defaultdict(set)
    by_name_street, by_name_zip, by_street_zip = defaultdict(list), defaultdict(list), defaultdict(list)
    for ln, k in ids.items():
        dsu.find(ln)
        if k["core"]:
            by_name_street[(k["core"], k["street"], k["zip5"])].append(ln)
            by_name_zip[(k["core"], k["zip5"])].append(ln)
        if k["street"] and k["zip5"]:
            by_street_zip[(k["street"], k["zip5"])].append(ln)
    for key, lns in by_name_street.items():
        for o in lns[1:]:
            dsu.union(lns[0], o)
            link_reason[lns[0]].add("same legal name + same street address + ZIP")
    for key, lns in by_name_zip.items():
        for o in lns[1:]:
            if dsu.find(o) != dsu.find(lns[0]):
                dsu.union(lns[0], o)
                link_reason[lns[0]].add("same legal name + same ZIP (street spelling differs)")
    # DBA evidence: a loan's documented DBA equals another loan's legal name at the same street+ZIP
    for key, lns in by_street_zip.items():
        for a in lns:
            for b in lns:
                if a != b and ids[b]["core"] and ids[b]["core"] in ids[a]["dba_cores"]:
                    dsu.union(a, b)
                    link_reason[a].add(f"documented DBA in SBA BorrowerName ('{ids[a]['dba_raw'][0]}') matches other loan's legal name at same address")
    groups = defaultdict(list)
    for ln in ids:
        groups[dsu.find(ln)].append(ln)
    review_notes = []
    # Same address, different legal names, no DBA evidence: kept separate but flagged.
    for key, lns in by_street_zip.items():
        roots = {dsu.find(x) for x in lns}
        if len(roots) > 1 and not key[0].startswith("PO BOX"):
            names = sorted({loans[x]["BorrowerName"] for x in lns})
            review_notes.append({"street_zip": f"{key[0]} {key[1]}", "loan_numbers": sorted(lns), "names": names})
    out = []
    for root, lns in groups.items():
        lns.sort(key=lambda x: (parse_date(loans[x]["DateApproved"]) or dt.date.min, x))
        reasons = set()
        for x in lns:
            reasons |= link_reason.get(x, set())
        out.append({"loans": lns, "ids": [ids[x] for x in lns], "link_reasons": sorted(reasons)})
    return out, review_notes


REG_FIELDS = ["biz_id", "identity_key", "status", "merged_into", "created_run", "last_seen_run", "loan_numbers"]


def assign_ids(groups, loans):
    _, reg = read_csv(P["registry"])
    _, prev_loans = read_csv(P["loans"])
    loan_to_biz = {r["loan_number"]: r["biz_id"] for r in prev_loans if r.get("biz_id")}
    key_to_biz = {}
    for r in reg:
        for k in (r.get("identity_key") or "").split(" || "):
            if k and r["status"] == "active":
                key_to_biz.setdefault(k, r["biz_id"])
    pref, width = CFG["id_prefix"], CFG["id_width"]
    nums = [int(r["biz_id"][len(pref):]) for r in reg if r["biz_id"].startswith(pref)]
    nxt = max(nums, default=0) + 1
    reg_by_id = {r["biz_id"]: r for r in reg}
    claimed, merges, new_ids = set(), [], []
    # deterministic order: oldest first loan date, then loan number
    groups.sort(key=lambda g: (parse_date(loans[g["loans"][0]]["DateApproved"]) or dt.date.min, g["loans"][0]))
    for g in groups:
        keys = sorted({f"{k['core']}|{k['street']}|{k['zip5']}" for k in g["ids"]})
        prior = sorted({loan_to_biz[x] for x in g["loans"] if x in loan_to_biz} - claimed)
        if not prior:
            prior = sorted({key_to_biz[k] for k in keys if k in key_to_biz} - claimed)
        if prior:
            bid = prior[0]
            for other in prior[1:]:
                merges.append((other, bid))
                reg_by_id[other]["status"] = "merged"
                reg_by_id[other]["merged_into"] = bid
                claimed.add(other)
        else:
            bid = f"{pref}{nxt:0{width}d}"
            nxt += 1
            new_ids.append(bid)
            reg_by_id[bid] = {"biz_id": bid, "status": "active", "merged_into": "", "created_run": RUN_ID}
        claimed.add(bid)
        r = reg_by_id[bid]
        old_keys = set(filter(None, (r.get("identity_key") or "").split(" || ")))
        old_loans = set(filter(None, (r.get("loan_numbers") or "").split(";")))
        r["identity_key"] = " || ".join(sorted(old_keys | set(keys)))
        r["loan_numbers"] = ";".join(sorted(old_loans | set(g["loans"])))
        r["last_seen_run"] = RUN_ID
        g["biz_id"] = bid
    write_csv(P["registry"], REG_FIELDS, sorted(reg_by_id.values(), key=lambda r: r["biz_id"]))
    return new_ids, merges


# --------------------------------------------------------------------------
# stage 3: Texas Active Franchise Taxpayers
# --------------------------------------------------------------------------
TX_COLS = {
    "Taxpayer Number": "taxpayer_number", "Taxpayer Name": "taxpayer_name", "Taxpayer Address": "taxpayer_address",
    "Taxpayer City": "taxpayer_city", "Taxpayer State": "taxpayer_state", "Taxpayer Zip": "taxpayer_zip",
    "Taxpayer Organizational Type": "org_type", "Record Type Code": "record_type_code",
    "Responsibility Beginning Date": "responsibility_beginning_date",
    "Secretary of State (SOS) or COA File Number": "sos_file_number", "SOS Charter Date": "sos_charter_date",
    "SOS Status Date": "sos_status_date", "SOS Status Code": "sos_status_code",
    "Right to Transact Business Code": "right_to_transact_code", "NAICS Code": "tx_naics",
}
RECORD_TYPE_MEANING = {
    "U": "Texas SOS charter (formation) date of a domestic Texas entity",
    "V": "Texas SOS Certificate of Authority date: when a foreign (out-of-state) entity registered in Texas; formation elsewhere may be earlier",
    "X": "Comptroller-assigned file number; no SOS charter date exists",
}


def stage_texas(groups, manifest):
    path = os.path.join(RAW, CFG["sources"]["TX_FRANCHISE_9CIR_EFMM"]["raw_file"])
    stats = {}
    if not os.path.exists(path):
        log_unavailable("TX_FRANCHISE_9CIR_EFMM", CFG["sources"]["TX_FRANCHISE_9CIR_EFMM"]["name"],
                        CFG["sources"]["TX_FRANCHISE_9CIR_EFMM"]["url"], "raw file not available this run")
        return {}, {"available": False}
    names, streets = set(), set()
    for g in groups:
        for k in g["ids"]:
            names.add(k["core"])
            names.update(k["dba_cores"])
            if k["street"] and k["zip5"]:
                streets.add((k["street"], k["zip5"]))
    names.discard("")
    dec = LineDecoder()
    by_name, by_street = defaultdict(list), defaultdict(list)
    n = 0
    with open(path, "rb") as fb:
        rdr = csv.reader(decoded_lines(fb, dec))
        header = [h.strip() for h in next(rdr)]
        ix = {TX_COLS[h]: i for i, h in enumerate(header) if h in TX_COLS}
        for row in rdr:
            n += 1
            rec = {k: row[i].strip() if i < len(row) else "" for k, i in ix.items()}
            cn = core_name(rec["taxpayer_name"])
            st = norm_street(rec["taxpayer_address"])
            z = zip5(rec["taxpayer_zip"])
            hit = False
            if cn in names:
                by_name[cn].append(rec)
                hit = True
            if (st, z) in streets and not hit:
                by_street[(st, z)].append(rec)
            if hit or (st, z) in streets:
                rec["_core"], rec["_street"], rec["_zip5"] = cn, st, z
    meta = (manifest.get("fetch", {}).get("files") or {}).get("TX_FRANCHISE_9CIR_EFMM") or {}
    stats = {"available": True, "data_rows": n, **dec.summary(), "sha256": meta.get("sha256") or sha256_file(path),
             "bytes": os.path.getsize(path), "dataset_rows_updated_at": (meta.get("dataset_metadata") or {}).get("rows_updated_at", ""),
             "retrieved_at_utc": meta.get("retrieved_at_utc", "")}
    log(f"TX: scanned {n:,} rows; {sum(map(len, by_name.values()))} name candidates")
    results = {}
    cand_rows = []
    for g in groups:
        bid = g["biz_id"]
        cores = {k["core"] for k in g["ids"] if k["core"]}
        fulls = {k["full"] for k in g["ids"]}
        dbas = {d for k in g["ids"] for d in k["dba_cores"] if d}
        streets_g = {(k["street"], k["zip5"]) for k in g["ids"]}
        houses = {(k["house"], k["zip5"]) for k in g["ids"] if k["house"]}
        zips = {k["zip5"] for k in g["ids"]}
        cities = {k["city"] for k in g["ids"]}
        ppp_suffixes = {suffix_of(k["legal_raw"]) for k in g["ids"]} - {""}
        seen, cands = set(), []
        pool = [(r, "legal") for c in cores for r in by_name.get(c, [])] + \
               [(r, "dba") for c in dbas for r in by_name.get(c, [])] + \
               [(r, "address") for s in streets_g for r in by_street.get(s, [])]
        for r, via in pool:
            if r["taxpayer_number"] in seen:
                continue
            seen.add(r["taxpayer_number"])
            street_hit = (r["_street"], r["_zip5"]) in streets_g or (house_key(r["_street"]), r["_zip5"]) in houses
            zip_hit = r["_zip5"] in zips
            city_hit = norm_text(r["taxpayer_city"]) in cities
            exact_full = norm_text(r["taxpayer_name"]) in fulls
            ev = []
            if via == "legal":
                ev.append(("exact legal name incl. entity suffix" if exact_full else "legal name match (entity suffix ignored)")
                          + f": TX '{r['taxpayer_name']}'")
            elif via == "dba":
                ev.append(f"TX legal name '{r['taxpayer_name']}' equals DBA documented in SBA BorrowerName")
            else:
                best = max(tokens_sim(r["_core"], c) for c in cores) if cores else 0
                if best < 0.5:
                    continue
                ev.append(f"same street+ZIP, similar name (token overlap {best:.2f}): TX '{r['taxpayer_name']}'")
            if street_hit:
                ev.append(f"street address corroborates: TX '{r['taxpayer_address']}, {r['taxpayer_city']} {r['taxpayer_zip']}'")
            elif zip_hit:
                ev.append(f"ZIP {r['_zip5']} corroborates (street differs: TX '{r['taxpayer_address']}')")
            elif city_hit:
                ev.append(f"city only ({r['taxpayer_city']}); street and ZIP differ")
            else:
                ev.append(f"no address corroboration (TX address {r['taxpayer_address']}, {r['taxpayer_city']} {r['taxpayer_state']} {r['taxpayer_zip']})")
            unique_name = via == "legal" and len(by_name.get(r["_core"], [])) == 1
            if via == "legal" and street_hit:
                conf = "high"
            elif via == "legal" and zip_hit and exact_full:
                conf = "medium"
            elif via == "legal" and city_hit and exact_full and unique_name and not street_hit:
                conf = "medium"
                ev.append("only active Texas record with this name statewide; city-level address corroboration")
            elif via == "dba" and street_hit:
                conf = "medium"
            else:
                conf = "low"
            tx_suffix = suffix_of(r["taxpayer_name"])
            suffix_conflict = via == "legal" and not exact_full and bool(ppp_suffixes) and bool(tx_suffix) and \
                not any(suffix_compatible(tx_suffix, s) for s in ppp_suffixes)
            if suffix_conflict:
                ev.append(f"entity type differs (PPP '{'/'.join(sorted(ppp_suffixes))}' vs TX '{tx_suffix or 'none'}'): possible conversion/successor, charter date may not cover the PPP entity")
            cands.append({**r, "confidence": conf, "via": via, "exact_full": exact_full, "suffix_conflict": suffix_conflict,
                          "evidence": "; ".join(ev)})
        rank = {"high": 3, "medium": 2, "low": 1}
        cands.sort(key=lambda c: (-rank[c["confidence"]], not c["exact_full"], c["taxpayer_number"]))
        for c in cands:
            cand_rows.append({"biz_id": bid, **{k: v for k, v in c.items() if not k.startswith("_")}})
        verified = [c for c in cands if c["confidence"] in ("high", "medium")]
        top = [c for c in verified if c["confidence"] == verified[0]["confidence"]] if verified else []
        if len(top) > 1 and sum(c["exact_full"] for c in top) == 1:
            top = [c for c in top if c["exact_full"]]
            top[0]["evidence"] += "; preferred over same-address namesake(s) with a different entity suffix"
        if len(top) == 1:
            results[bid] = {"status": "verified", "match": top[0], "candidates": cands,
                            "note": "" if len(verified) == 1 else f"{len(verified) - 1} other corroborated candidate(s) seen, see data/tx_franchise_candidates.csv"}
        elif len(top) > 1:
            results[bid] = {"status": "review", "match": None, "candidates": cands,
                            "note": f"ambiguous: {len(top)} Texas records with equal '{top[0]['confidence']}' evidence"}
        elif cands:
            results[bid] = {"status": "review", "match": None, "candidates": cands,
                            "note": "name match without street/ZIP corroboration" if any(c["via"] != "address" for c in cands)
                            else "same address, similar but not equal name"}
        else:
            results[bid] = {"status": "unmatched", "match": None, "candidates": [], "note": "no Texas active franchise taxpayer record with matching name"}
    stats["candidate_rows"] = len(cand_rows)
    return results, stats


def write_tx_candidates(tx_results):
    rows = []
    for bid in sorted(tx_results):
        tr = tx_results[bid]
        for c in tr["candidates"]:
            chosen = "selected" if tr.get("match") is c else ""
            rows.append({"biz_id": bid, "tx_match_status": tr["status"], "selected": chosen,
                         **{k: v for k, v in c.items() if not k.startswith("_")}})
    write_csv(P["tx_candidates"], ["biz_id", "tx_match_status", "selected", "confidence", "via", "exact_full", "suffix_conflict", "taxpayer_number",
                                    "taxpayer_name", "taxpayer_address", "taxpayer_city", "taxpayer_state", "taxpayer_zip",
                                    "org_type", "record_type_code", "sos_file_number", "sos_charter_date", "sos_status_code",
                                    "right_to_transact_code", "responsibility_beginning_date", "tx_naics", "evidence"], rows)


def ein_from_tx_taxpayer_number(tpn):
    """Texas taxpayer numbers that start with '1' are the FEIN prefixed by 1 plus a check digit."""
    d = re.sub(r"\D", "", tpn or "")
    return d[1:10] if len(d) == 11 and d[0] == "1" else ""


# --------------------------------------------------------------------------
# stage 4: DOL Form 5500 / 5500-SF
# --------------------------------------------------------------------------
F5500 = {"ack": "ACK_ID", "begin": "FORM_PLAN_YEAR_BEGIN_DATE", "end": "FORM_TAX_PRD", "pn": "SPONS_DFE_PN",
         "plan_name": "PLAN_NAME", "sponsor": "SPONSOR_DFE_NAME", "dba": "SPONS_DFE_DBA_NAME", "ein": "SPONS_DFE_EIN",
         "addr1": "SPONS_DFE_MAIL_US_ADDRESS1", "city": "SPONS_DFE_MAIL_US_CITY", "state": "SPONS_DFE_MAIL_US_STATE",
         "zip": "SPONS_DFE_MAIL_US_ZIP", "loc_addr1": "SPONS_DFE_LOC_US_ADDRESS1", "loc_zip": "SPONS_DFE_LOC_US_ZIP",
         "pension": "TYPE_PENSION_BNFT_CODE", "active": "TOT_ACTIVE_PARTCP_CNT", "received": "DATE_RECEIVED",
         "entity": "TYPE_PLAN_ENTITY_CD", "status": "FILING_STATUS", "business_code": "BUSINESS_CODE",
         "final": "FINAL_FILING_IND", "amended": "AMENDED_IND"}
FSF = {"ack": "ACK_ID", "begin": "SF_PLAN_YEAR_BEGIN_DATE", "end": "SF_TAX_PRD", "pn": "SF_PLAN_NUM",
       "plan_name": "SF_PLAN_NAME", "sponsor": "SF_SPONSOR_NAME", "dba": "SF_SPONSOR_DFE_DBA_NAME", "ein": "SF_SPONS_EIN",
       "addr1": "SF_SPONS_US_ADDRESS1", "city": "SF_SPONS_US_CITY", "state": "SF_SPONS_US_STATE", "zip": "SF_SPONS_US_ZIP",
       "loc_addr1": "SF_SPONS_LOC_US_ADDRESS1", "loc_zip": "SF_SPONS_LOC_US_ZIP", "pension": "SF_TYPE_PENSION_BNFT_CODE",
       "active": "SF_TOT_ACT_PARTCP_EOY_CNT", "received": "DATE_RECEIVED", "entity": "SF_PLAN_ENTITY_CD",
       "status": "FILING_STATUS", "business_code": "SF_BUSINESS_CODE", "final": "SF_FINAL_FILING_IND",
       "amended": "SF_AMENDED_IND"}


def retirement_codes(code_str):
    codes = re.findall(r"[0-9][A-Z]", (code_str or "").upper())
    return [c for c in codes if c[0] in "12"]


def pension_targets(groups, tx_results, verified_only):
    ein_to_biz, name_to_biz, biz_addr = defaultdict(set), defaultdict(set), {}
    for g in groups:
        bid = g["biz_id"]
        for k in g["ids"]:
            if k["core"]:
                name_to_biz[k["core"]].add(bid)
            for d in k["dba_cores"]:
                if d:
                    name_to_biz[d].add(bid)
        biz_addr[bid] = ({(k["street"], k["zip5"]) for k in g["ids"]}, {(k["house"], k["zip5"]) for k in g["ids"] if k["house"]},
                         {k["zip5"] for k in g["ids"]}, {k["core"] for k in g["ids"]} | {d for k in g["ids"] for d in k["dba_cores"]})
        tr = tx_results.get(bid) or {}
        recs = [tr["match"]] if tr.get("status") == "verified" else ([] if verified_only else tr.get("candidates", []))
        for m in recs:
            name_to_biz[m["_core"]].add(bid)
            e = ein_from_tx_taxpayer_number(m["taxpayer_number"])
            if e:
                ein_to_biz[e].add(bid)
            if tr.get("status") == "verified":
                biz_addr[bid][0].add((m["_street"], m["_zip5"]))
                biz_addr[bid][1].add((house_key(m["_street"]), m["_zip5"]))
                biz_addr[bid][2].add(m["_zip5"])
                biz_addr[bid][3].add(m["_core"])
    return ein_to_biz, name_to_biz, biz_addr


def sponsor_vs_business(h, streets, houses, zips, names):
    s1, s2 = norm_street(h.get("addr1")), norm_street(h.get("loc_addr1"))
    z1, z2 = zip5(h.get("zip")), zip5(h.get("loc_zip"))
    addr_hit = any(((s, z) in streets) or ((house_key(s), z) in houses) for s, z in ((s1, z1), (s2, z2)) if s)
    zip_hit = z1 in zips or z2 in zips
    name_hit = h["_sc"] in names or bool(h["_dc"] and h["_dc"] in names)
    return addr_hit, zip_hit, name_hit


def corroborate_texas_via_dol(groups, tx_results, hits):
    """A name-only Texas candidate is upgraded when a DOL filing ties the two together:
    sponsor EIN == FEIN inside the TX taxpayer number, sponsor name == TX name, sponsor address == PPP address."""
    upgraded = 0
    for g in groups:
        bid = g["biz_id"]
        tr = tx_results.get(bid)
        if not tr or tr["status"] != "review":
            continue
        named = [c for c in tr["candidates"] if c["via"] in ("legal", "dba")]
        if len(named) != 1:
            continue
        c = named[0]
        fein = ein_from_tx_taxpayer_number(c["taxpayer_number"])
        if not fein:
            continue
        streets = {(k["street"], k["zip5"]) for k in g["ids"]}
        houses = {(k["house"], k["zip5"]) for k in g["ids"] if k["house"]}
        for h in hits:
            if h["ein9"] != fein or h["_sc"] != c["_core"]:
                continue
            addr_hit, _, _ = sponsor_vs_business(h, streets, houses, set(), set())
            if addr_hit:
                c["confidence"] = "medium"
                c["evidence"] += (f"; address corroborated via DOL Form {h['form']} {h['dataset_year']} ACK {h.get('ack')}: sponsor EIN {fein}"
                                  f" (= FEIN in TX taxpayer number), sponsor '{h.get('sponsor')}' at {h.get('addr1')}, {h.get('city')} {h.get('zip')}"
                                  f" = PPP borrower address")
                tr.update({"status": "verified", "match": c, "note": "", "via_dol": True})
                upgraded += 1
                break
    return upgraded


def scan_dol(groups, tx_results, manifest):
    files = (manifest.get("fetch", {}).get("files") or {})
    years = manifest.get("fetch", {}).get("dol_plan_years") or []
    ein_to_biz, name_to_biz, _ = pension_targets(groups, tx_results, verified_only=False)
    stats = {"plan_years": years, "files": {}}
    hits = []
    for sid, cols, form in (("DOL_FORM_5500", F5500, "5500"), ("DOL_FORM_5500_SF", FSF, "5500-SF")):
        for y in years:
            key = f"{sid}_{y}"
            meta = files.get(key)
            path = os.path.join(RAW, CFG["sources"][sid]["raw_file_template"].format(year=y))
            if not meta or not os.path.exists(path):
                log_unavailable(key, f"{CFG['sources'][sid]['name']} {y}", CFG["sources"][sid]["url_template"].format(year=y),
                                "file not downloaded")
                continue
            dec = LineDecoder()
            c = Counter()
            with zipfile.ZipFile(path) as z:
                member = [n for n in z.namelist() if n.lower().endswith(".csv")][0]
                with z.open(member) as fb:
                    rdr = csv.reader(decoded_lines(fb, dec))
                    header = next(rdr)
                    ix = {k: header.index(v) for k, v in cols.items() if v in header}
                    for row in rdr:
                        c["data_rows"] += 1
                        rec = {k: (row[i].strip() if i < len(row) else "") for k, i in ix.items()}
                        ein = re.sub(r"\D", "", rec.get("ein", "")).zfill(9)
                        sc, dc = core_name(rec.get("sponsor", "")), core_name(rec.get("dba", ""))
                        bids = set(ein_to_biz.get(ein, ())) | name_to_biz.get(sc, set()) | (name_to_biz.get(dc, set()) if dc else set())
                        if not bids:
                            continue
                        c["candidate_rows"] += 1
                        rc = retirement_codes(rec.get("pension"))
                        if not rc:
                            c["candidate_rows_without_retirement_benefit_code"] += 1
                            continue
                        if form == "5500" and rec.get("entity") == "4":
                            c["candidate_rows_dfe_skipped"] += 1
                            continue
                        rec.update({"form": form, "dataset_year": y, "source_id": key, "ein9": ein, "retirement_codes": " ".join(rc),
                                    "_sc": sc, "_dc": dc,
                                    "source_url": meta.get("url", ""), "source_sha256": meta.get("sha256", "")})
                        hits.append(rec)
            stats["files"][key] = {"member": member, **dict(c), **dec.summary(), "sha256": meta.get("sha256"),
                                   "bytes": meta.get("bytes"), "retrieved_at_utc": meta.get("retrieved_at_utc")}
            log(f"DOL {key}: {c['data_rows']:,} rows scanned, {c['candidate_rows']} candidate rows")
    return hits, stats


def match_pension(groups, tx_results, hits, stats):
    ein_to_biz, name_to_biz, biz_addr = pension_targets(groups, tx_results, verified_only=True)
    per_biz = defaultdict(lambda: {"plans": {}, "review": []})
    for h in hits:
        bids = set(ein_to_biz.get(h["ein9"], ())) | name_to_biz.get(h["_sc"], set()) | (name_to_biz.get(h["_dc"], set()) if h["_dc"] else set())
        for bid in bids:
            streets, houses, zips, names = biz_addr.get(bid, (set(), set(), set(), set()))
            addr_hit, zip_hit, name_hit = sponsor_vs_business(h, streets, houses, zips, names)
            ein_hit = bid in ein_to_biz.get(h["ein9"], set())
            ev = []
            if ein_hit:
                ev.append(f"sponsor EIN {h['ein9']} equals FEIN embedded in verified Texas taxpayer number")
            if name_hit:
                ev.append(f"sponsor name '{h.get('sponsor')}'" + (f" / DBA '{h.get('dba')}'" if h.get("dba") else "") + " matches")
            if addr_hit:
                ev.append(f"sponsor address corroborates ({h.get('addr1')}, {h.get('city')} {h.get('state')} {h.get('zip')})")
            elif zip_hit:
                ev.append(f"sponsor ZIP corroborates ({h.get('city')} {h.get('state')} {h.get('zip')})")
            else:
                ev.append(f"sponsor address differs ({h.get('addr1')}, {h.get('city')} {h.get('state')} {h.get('zip')})")
            if ein_hit and name_hit:
                conf = "high"
            elif name_hit and addr_hit:
                conf = "high" if ein_hit else "medium"
            elif ein_hit and (addr_hit or zip_hit):
                conf = "medium"
            elif name_hit and zip_hit:
                conf = "medium"
            else:
                conf = "low"
            plan_id = f"{h['ein9']}-{h.get('pn', '').zfill(3)}"
            rec = {"biz_id": bid, "plan_id": plan_id, "ack_id": h.get("ack"), "form": h["form"], "dataset_year": h["dataset_year"],
                   "plan_name": h.get("plan_name"), "sponsor_name": h.get("sponsor"), "sponsor_dba": h.get("dba"),
                   "sponsor_ein": h["ein9"], "sponsor_state": h.get("state"), "plan_year_begin": h.get("begin"),
                   "plan_year_end": h.get("end"), "filing_date_received": h.get("received"),
                   "active_participants_eoy": h.get("active"), "retirement_codes": h["retirement_codes"],
                   "pension_codes_all": h.get("pension"), "final_filing": h.get("final"), "amended": h.get("amended"),
                   "filing_status": h.get("status"), "match_confidence": conf, "match_evidence": "; ".join(ev),
                   "source_id": h["source_id"], "source_url": h["source_url"], "source_sha256": h["source_sha256"]}
            if conf == "low":
                per_biz[bid]["review"].append(rec)
                continue
            cur = per_biz[bid]["plans"].get(plan_id)
            if cur is None or (rec["plan_year_begin"], rec["filing_date_received"]) > (cur["plan_year_begin"], cur["filing_date_received"]):
                per_biz[bid]["plans"][plan_id] = rec
    rows = []
    for bid, d in per_biz.items():
        rows.extend(sorted(d["plans"].values(), key=lambda r: r["plan_id"]))
        for r in d["review"]:
            rows.append({**r, "match_confidence": "low (review)"})
    write_csv(P["plans"], ["biz_id", "plan_id", "match_confidence", "match_evidence", "form", "dataset_year", "ack_id",
                           "plan_name", "sponsor_name", "sponsor_dba", "sponsor_ein", "sponsor_state", "plan_year_begin",
                           "plan_year_end", "filing_date_received", "active_participants_eoy", "retirement_codes",
                           "pension_codes_all", "final_filing", "amended", "filing_status", "source_id", "source_url",
                           "source_sha256"], rows)
    stats["eins_known_from_texas"] = len(ein_to_biz)
    stats["matched_plan_rows"] = sum(len(d["plans"]) for d in per_biz.values())
    stats["low_confidence_plan_rows_review"] = sum(len(d["review"]) for d in per_biz.values())
    return per_biz, stats


# --------------------------------------------------------------------------
# stage 5: assemble tracker
# --------------------------------------------------------------------------
OWNED_COLUMNS = [
    "biz_id", "legal_name", "dba_names", "address", "city", "state", "zip5", "borrower_names_seen", "sba_business_type",
    "identity_evidence",
    "loan_count", "loan_numbers", "draws", "loan_amounts_current", "loan_approval_dates", "loan_statuses",
    "lenders", "jobs_reported_by_loan", "historical_payroll_proxy_annual", "payroll_proxy_basis",
    "tx_match_status", "tx_match_confidence", "tx_match_evidence", "tx_taxpayer_number", "tx_taxpayer_name",
    "tx_org_type", "tx_sos_file_number", "tx_right_to_transact_code", "record_date", "record_date_meaning",
    "record_date_source", "record_age_years", "record_age_30plus",
    "pension_status", "pension_plan_ids", "pension_plan_periods", "pension_filing_dates",
    "pension_active_participants_by_plan", "pension_largest_single_plan_active_participants", "pension_match_evidence",
    "eligibility_status", "eligibility_reason", "evidence_links", "first_seen_run", "last_seen_run", "in_current_source",
]
PRESERVED_DEFAULTS = ["outreach_status"]
PROXY_BUSINESS_TYPES = {"Corporation", "Subchapter S Corporation"}


def payroll_proxy(rec):
    amt = money(rec["CurrentApprovalAmount"])
    init = money(rec["InitialApprovalAmount"])
    pm = rec["ProcessingMethod"].strip()
    jobs = money(rec["JobsReported"])
    reasons = []
    if pm not in ("PPP", "PPS"):
        reasons.append(f"processing method {pm}")
    if rec["NAICSCode"].startswith("72"):
        reasons.append("NAICS 72 uses 3.5x")
    if rec["BusinessType"] not in PROXY_BUSINESS_TYPES:
        reasons.append(f"business type '{rec['BusinessType']}' may use owner income/SE basis")
    if money(rec.get("REFINANCE_EIDL_PROCEED")) > 0:
        reasons.append("EIDL refinance added to loan")
    if abs(amt - init) > 0.005:
        reasons.append("current amount differs from initial approval")
    cap = 10_000_000 if pm == "PPP" else 2_000_000
    if amt >= cap:
        reasons.append("at program cap")
    annual = amt / 2.5 * 12
    if jobs <= 0:
        reasons.append("JobsReported missing, cannot test $100K/employee cap")
    elif annual / jobs >= 100_000:
        reasons.append("implied pay per reported job reaches $100K cap")
    if reasons:
        return None, "; ".join(reasons)
    return round(annual), f"{rec['LoanNumber']} ({pm}, {rec['DateApproved']}): {amt:,.2f} / 2.5 x 12"


def build_tracker(groups, loans, tx_results, pension, sba_stats, new_ids, merges, review_notes):
    old_fields, old_rows = read_csv(TRACKER)
    old = {r["biz_id"]: r for r in old_rows}
    for other, into in merges:
        if other in old and into in old:
            for k, v in old[other].items():
                if k not in OWNED_COLUMNS and v and not old[into].get(k):
                    old[into][k] = v
        elif other in old:
            old[into] = {**old.pop(other), "biz_id": into}
        old.pop(other, None)
    extra_cols = [c for c in old_fields if c not in OWNED_COLUMNS]
    for c in PRESERVED_DEFAULTS:
        if c not in extra_cols:
            extra_cols.insert(0, c)
    sba_url = sba_stats["source_url"]
    tx_rec_url = CFG["sources"]["TX_FRANCHISE_9CIR_EFMM"]["record_url"]
    rows, review_rows, ledger = [], [], []
    seen_loans = set()
    for g in groups:
        bid = g["biz_id"]
        recs = [loans[x] for x in g["loans"]]
        latest = recs[-1]
        legal, _ = split_dba(latest["BorrowerName"])
        dbas = sorted({d for k in g["ids"] for d in k["dba_raw"]})
        proxy, basis = None, ""
        for pm in ("PPP", "PPS"):
            for r in recs:
                if r["ProcessingMethod"] == pm and proxy is None:
                    v, why = payroll_proxy(r)
                    if v is not None:
                        proxy, basis = v, why
                    elif not basis:
                        basis = f"unknown: {r['LoanNumber']}: {why}"
        if proxy is None and not basis:
            basis = "unknown"
        evidence = g["link_reasons"] or (["single loan"] if len(recs) == 1 else [])
        row = {
            "biz_id": bid, "legal_name": legal, "dba_names": "; ".join(dbas), "address": latest["BorrowerAddress"],
            "city": latest["BorrowerCity"], "state": latest["BorrowerState"], "zip5": zip5(latest["BorrowerZip"]),
            "borrower_names_seen": " | ".join(sorted({r["BorrowerName"] for r in recs})),
            "sba_business_type": " | ".join(sorted({r["BusinessType"] for r in recs if r["BusinessType"]})),
            "identity_evidence": "; ".join(evidence),
            "loan_count": len(recs), "loan_numbers": ";".join(r["LoanNumber"] for r in recs),
            "draws": ";".join(f"{r['LoanNumber']}:{'first draw' if r['ProcessingMethod'] == 'PPP' else 'second draw' if r['ProcessingMethod'] == 'PPS' else r['ProcessingMethod']}" for r in recs),
            "loan_amounts_current": ";".join(f"{r['LoanNumber']}:{money(r['CurrentApprovalAmount']):.2f}" for r in recs),
            "loan_approval_dates": ";".join(f"{r['LoanNumber']}:{r['DateApproved']}" for r in recs),
            "loan_statuses": ";".join(f"{r['LoanNumber']}:{r['LoanStatus']}" for r in recs),
            "lenders": " | ".join(sorted({r["OriginatingLender"] for r in recs})),
            "jobs_reported_by_loan": ";".join(f"{r['LoanNumber']}:{r['JobsReported'] or 'blank'}" for r in recs),
            "historical_payroll_proxy_annual": proxy if proxy is not None else "unknown",
            "payroll_proxy_basis": basis,
            "first_seen_run": old.get(bid, {}).get("first_seen_run") or RUN_ID, "last_seen_run": RUN_ID,
            "in_current_source": "yes",
        }
        for r in recs:
            seen_loans.add(r["LoanNumber"])
            ledger.append({"biz_id": bid, "source_id": "SBA_PPP_150K_PLUS", "source_record_id": r["LoanNumber"],
                           "source_name": CFG["sources"]["SBA_PPP_150K_PLUS"]["name"], "url": sba_url,
                           "retrieved_date": sba_stats.get("retrieved_at_utc", "")[:10], "source_sha256": sba_stats["sha256"],
                           "reporting_period": f"loan approved {r['DateApproved']}; SBA data as of {sba_stats['data_as_of']}",
                           "match_method": "filter BorrowerState=TX, NAICSCode=238220, CurrentApprovalAmount>=150000",
                           "match_evidence": f"{r['BorrowerName']}, {r['BorrowerAddress']}, {r['BorrowerCity']} {r['BorrowerZip']}; group: {'; '.join(evidence)}",
                           "match_confidence": "source record"})
        # Texas registration
        tr = tx_results.get(bid, {"status": "not_checked", "candidates": [], "note": "Texas source unavailable"})
        row["tx_match_status"] = tr["status"]
        m = tr.get("match")
        if m:
            cd = parse_date(m["sos_charter_date"])
            rt = (m["record_type_code"] or "").strip()
            row.update({
                "tx_match_confidence": m["confidence"], "tx_match_evidence": m["evidence"] + (f"; {tr['note']}" if tr["note"] else ""),
                "tx_taxpayer_number": m["taxpayer_number"], "tx_taxpayer_name": m["taxpayer_name"], "tx_org_type": m["org_type"],
                "tx_sos_file_number": m["sos_file_number"], "tx_right_to_transact_code": m["right_to_transact_code"],
                "record_date": cd.isoformat() if cd else "unknown",
                "record_date_meaning": RECORD_TYPE_MEANING.get(rt, f"record type '{rt}'") if cd else RECORD_TYPE_MEANING.get(rt, "no SOS charter date"),
                "record_date_source": "TX Comptroller Active Franchise Taxpayers (9cir-efmm) field 'SOS Charter Date'" if cd else "",
                "record_age_years": years_between(cd, TODAY) if cd else "unknown",
                "record_age_30plus": ("yes" if years_between(cd, TODAY) >= CFG["age_threshold_years"] else "no") if cd else "unknown",
            })
            ledger.append({"biz_id": bid, "source_id": "TX_FRANCHISE_9CIR_EFMM", "source_record_id": m["taxpayer_number"],
                           "source_name": CFG["sources"]["TX_FRANCHISE_9CIR_EFMM"]["name"], "url": tx_rec_url.format(id=m["taxpayer_number"]),
                           "retrieved_date": TX_STATS.get("retrieved_at_utc", "")[:10], "source_sha256": TX_STATS.get("sha256", ""),
                           "reporting_period": f"active taxpayers snapshot, dataset rows updated {TX_STATS.get('dataset_rows_updated_at', '')[:10]}",
                           "match_method": f"name + address join ({m['via']})", "match_evidence": m["evidence"],
                           "match_confidence": m["confidence"]})
        else:
            row.update({"tx_match_confidence": "", "tx_match_evidence": tr["note"], "record_date": "unknown",
                        "record_date_meaning": "", "record_date_source": "", "record_age_years": "unknown", "record_age_30plus": "unknown"})
        for c in tr.get("candidates", []):
            if tr["status"] == "review":
                review_rows.append({"biz_id": bid, "stage": "texas_registration", "reason": tr["note"], "candidate_id": c["taxpayer_number"],
                                    "candidate_name": c["taxpayer_name"], "candidate_date": c["sos_charter_date"],
                                    "confidence": c["confidence"], "evidence": c["evidence"],
                                    "url": tx_rec_url.format(id=c["taxpayer_number"])})
        # pension
        pb = pension.get(bid)
        plans = sorted((pb or {}).get("plans", {}).values(), key=lambda r: r["plan_id"])
        if plans:
            row["pension_status"] = "filing found"
            row["pension_plan_ids"] = ";".join(p["plan_id"] for p in plans)
            row["pension_plan_periods"] = ";".join(f"{p['plan_id']}:{p['plan_year_begin']}..{p['plan_year_end']}" for p in plans)
            row["pension_filing_dates"] = ";".join(f"{p['plan_id']}:{p['filing_date_received']}" for p in plans)
            row["pension_active_participants_by_plan"] = ";".join(f"{p['plan_id']}:{p['active_participants_eoy'] or 'blank'}" for p in plans)
            vals = [int(money(p["active_participants_eoy"])) for p in plans if p["active_participants_eoy"]]
            row["pension_largest_single_plan_active_participants"] = max(vals) if vals else "unknown"
            row["pension_match_evidence"] = " | ".join(f"{p['plan_id']} ({p['match_confidence']}): {p['match_evidence']}" for p in plans)
            for p in plans:
                ledger.append({"biz_id": bid, "source_id": p["source_id"], "source_record_id": f"{p['plan_id']} ACK {p['ack_id']}",
                               "source_name": f"DOL Form {p['form']} dataset {p['dataset_year']} (Latest)", "url": p["source_url"],
                               "retrieved_date": PENSION_STATS["files"].get(p["source_id"], {}).get("retrieved_at_utc", "")[:10],
                               "source_sha256": p["source_sha256"],
                               "reporting_period": f"plan year {p['plan_year_begin']}..{p['plan_year_end']}; received {p['filing_date_received']}",
                               "match_method": "EIN and/or sponsor name + address", "match_evidence": p["match_evidence"],
                               "match_confidence": p["match_confidence"]})
        else:
            row["pension_status"] = "unknown (no matched retirement-plan filing in searched years)"
            for k in ("pension_plan_ids", "pension_plan_periods", "pension_filing_dates", "pension_active_participants_by_plan",
                      "pension_match_evidence"):
                row[k] = ""
            row["pension_largest_single_plan_active_participants"] = "unknown"
        for p in (pb or {}).get("review", []):
            review_rows.append({"biz_id": bid, "stage": "pension", "reason": "sponsor name or EIN match without corroboration",
                                "candidate_id": p["plan_id"], "candidate_name": p["sponsor_name"],
                                "candidate_date": p["plan_year_begin"], "confidence": "low", "evidence": p["match_evidence"],
                                "url": p["source_url"]})
        # eligibility
        if tr["status"] == "verified" and m.get("suffix_conflict"):
            elig, why = "review", f"Texas record {row['record_date']} matched but entity type differs from PPP borrower; confirm conversion history"
        elif tr["status"] == "verified" and row["record_age_30plus"] == "yes":
            elig, why = "ready", f"verified Texas record ({row['tx_match_confidence']}) with record date {row['record_date']} ({row['record_age_years']} years)"
        elif tr["status"] == "verified" and row["record_age_30plus"] == "no":
            elig, why = "closed", f"verified Texas record dated {row['record_date']} is under {CFG['age_threshold_years']} years"
        elif tr["status"] == "verified":
            elig, why = "review", "verified Texas record but no SOS charter date"
        elif tr["status"] == "review":
            elig, why = "review", f"uncertain Texas join: {tr['note']}"
        else:
            elig, why = "review", f"record date unknown: {tr['note']}"
            if any(t in row["sba_business_type"] for t in ("Sole Proprietorship", "Self-Employed", "Independent Contractor")):
                why += " (sole proprietors are not franchise taxpayers)"
        row["eligibility_status"], row["eligibility_reason"] = elig, why
        links = [f"SBA PPP CSV (LoanNumber {';'.join(r['LoanNumber'] for r in recs)}) {sba_url}"]
        if m:
            links.append(f"Texas record {tx_rec_url.format(id=m['taxpayer_number'])}")
        for p in plans:
            links.append(f"DOL {p['form']} {p['dataset_year']} ACK {p['ack_id']} {p['source_url']}")
        row["evidence_links"] = " | ".join(links)
        prev = old.get(bid)
        for c in extra_cols:
            if prev is not None:
                row[c] = prev.get(c, "")
            elif c == "outreach_status":
                row[c] = "ready" if bid in new_ids and elig == "ready" else ("not_ready" if bid in new_ids else "")
            else:
                row[c] = ""
        rows.append(row)
    # businesses seen in earlier runs but absent now are kept, never deleted
    current = {r["biz_id"] for r in rows}
    for bid, prev in old.items():
        if bid not in current:
            rows.append({**prev, "in_current_source": "no"})
    for n in review_notes:
        review_rows.append({"biz_id": "", "stage": "business_grouping",
                            "reason": "same street address + ZIP, different legal names, no DBA evidence: kept as separate businesses",
                            "candidate_id": ";".join(n["loan_numbers"]), "candidate_name": " | ".join(n["names"]),
                            "candidate_date": "", "confidence": "", "evidence": n["street_zip"], "url": sba_url})
    rows.sort(key=lambda r: r["biz_id"])
    fields = OWNED_COLUMNS[:OWNED_COLUMNS.index("eligibility_reason") + 1] + extra_cols + \
        OWNED_COLUMNS[OWNED_COLUMNS.index("eligibility_reason") + 1:]
    write_csv(TRACKER, fields, rows)
    write_csv(P["review"], ["biz_id", "stage", "reason", "candidate_id", "candidate_name", "candidate_date", "confidence",
                            "evidence", "url"], review_rows)
    return rows, review_rows, ledger, seen_loans


LEDGER_FIELDS = ["biz_id", "source_id", "source_record_id", "source_name", "url", "retrieved_date", "source_sha256",
                 "reporting_period", "match_method", "match_evidence", "match_confidence", "first_logged_run",
                 "last_confirmed_run", "current"]


def write_ledger(ledger):
    _, old = read_csv(P["ledger"])
    key = lambda r: (r["biz_id"], r["source_id"], r["source_record_id"])  # noqa: E731
    merged = {key(r): {**r, "current": "no"} for r in old}
    for r in ledger:
        k = key(r)
        first = merged.get(k, {}).get("first_logged_run") or RUN_ID
        merged[k] = {**r, "first_logged_run": first, "last_confirmed_run": RUN_ID, "current": "yes"}
    write_csv(P["ledger"], LEDGER_FIELDS, sorted(merged.values(), key=lambda r: (r["biz_id"], r["source_id"], r["source_record_id"])))
    return len(merged)


LOAN_FIELDS = ["loan_number", "biz_id", "draw", "processing_method", "date_approved", "borrower_name", "borrower_address",
               "borrower_city", "borrower_state", "borrower_zip", "naics_code", "business_type", "initial_approval_amount",
               "current_approval_amount", "loan_status", "jobs_reported", "payroll_proceed", "refinance_eidl_proceed",
               "forgiveness_amount", "originating_lender", "historical_payroll_proxy_annual", "payroll_proxy_basis",
               "in_current_source", "first_seen_run", "last_seen_run"]


def write_loans(groups, loans):
    _, old = read_csv(P["loans"])
    old_by = {r["loan_number"]: r for r in old}
    out = {}
    for g in groups:
        for ln in g["loans"]:
            r = loans[ln]
            v, why = payroll_proxy(r)
            out[ln] = {"loan_number": ln, "biz_id": g["biz_id"],
                       "draw": {"PPP": "first draw", "PPS": "second draw"}.get(r["ProcessingMethod"], r["ProcessingMethod"]),
                       "processing_method": r["ProcessingMethod"], "date_approved": r["DateApproved"],
                       "borrower_name": r["BorrowerName"], "borrower_address": r["BorrowerAddress"], "borrower_city": r["BorrowerCity"],
                       "borrower_state": r["BorrowerState"], "borrower_zip": r["BorrowerZip"], "naics_code": r["NAICSCode"],
                       "business_type": r["BusinessType"], "initial_approval_amount": r["InitialApprovalAmount"],
                       "current_approval_amount": r["CurrentApprovalAmount"], "loan_status": r["LoanStatus"],
                       "jobs_reported": r["JobsReported"], "payroll_proceed": r["PAYROLL_PROCEED"],
                       "refinance_eidl_proceed": r["REFINANCE_EIDL_PROCEED"], "forgiveness_amount": r["ForgivenessAmount"],
                       "originating_lender": r["OriginatingLender"],
                       "historical_payroll_proxy_annual": v if v is not None else "unknown", "payroll_proxy_basis": why,
                       "in_current_source": "yes", "first_seen_run": old_by.get(ln, {}).get("first_seen_run") or RUN_ID,
                       "last_seen_run": RUN_ID}
    for ln, r in old_by.items():
        if ln not in out:
            out[ln] = {**r, "in_current_source": "no"}
    write_csv(P["loans"], LOAN_FIELDS, sorted(out.values(), key=lambda r: (r["biz_id"], r["loan_number"])))


TX_STATS, PENSION_STATS = {}, {"files": {}}


def cmd_build():
    ensure_dirs()
    manifest = load_manifest()
    for u in CFG["known_unavailable_sources"]:
        log_unavailable(u["source_id"], u["name"], u["checked_url"], u["reason"])
    loans, sba_stats = stage_sba(manifest)
    sba_stats["retrieved_at_utc"] = ((manifest.get("fetch", {}).get("files") or {}).get("SBA_PPP_150K_PLUS") or {}).get("retrieved_at_utc", "")
    groups, review_notes = group_loans(loans)
    new_ids, merges = assign_ids(groups, loans)
    tx_results, tx_stats = stage_texas(groups, manifest)
    TX_STATS.update(tx_stats)
    hits, pstats = scan_dol(groups, tx_results, manifest)
    tx_stats["upgraded_via_dol_ein_address"] = corroborate_texas_via_dol(groups, tx_results, hits)
    pension, pstats = match_pension(groups, tx_results, hits, pstats)
    write_tx_candidates(tx_results)
    PENSION_STATS.update(pstats)
    rows, review_rows, ledger, _ = build_tracker(groups, loans, tx_results, pension, sba_stats, new_ids, merges, review_notes)
    write_loans(groups, loans)
    n_ledger = write_ledger(ledger)
    cur = [r for r in rows if r.get("in_current_source") == "yes"]
    cnt = lambda col: dict(Counter(r[col] for r in cur))  # noqa: E731
    coverage = {
        "run_id": RUN_ID, "as_of_date": TODAY.isoformat(),
        "sba": {k: v for k, v in sba_stats.items() if k != "excluded_blank_borrowerstate_loan_numbers"},
        "sba_blank_borrowerstate_projectstate_tx_loans": sba_stats["excluded_blank_borrowerstate_loan_numbers"],
        "business_groups": len(groups), "multi_loan_groups": sum(1 for g in groups if len(g["loans"]) > 1),
        "new_ids_this_run": new_ids, "id_merges_this_run": merges,
        "texas": tx_stats, "texas_match_status": cnt("tx_match_status"),
        "texas_match_confidence": cnt("tx_match_confidence"), "record_age_30plus": cnt("record_age_30plus"),
        "pension": pstats, "pension_status": cnt("pension_status"),
        "payroll_proxy_known": sum(1 for r in cur if r["historical_payroll_proxy_annual"] != "unknown"),
        "eligibility_status": cnt("eligibility_status"), "outreach_status": cnt("outreach_status"),
        "review_queue_rows": len(review_rows), "review_queue_by_stage": dict(Counter(r["stage"] for r in review_rows)),
        "ledger_rows": n_ledger, "tracker_rows": len(rows),
    }
    json.dump(coverage, open(P["coverage"], "w"), indent=1)
    manifest["build"] = {"run_id": RUN_ID, "sba": coverage["sba"], "texas": tx_stats, "pension": pstats}
    save_manifest(manifest)
    append_csv(P["runs"], ["run_id", "sba_source_rows", "matching_loans", "business_groups", "new_ids", "ready", "review",
                           "closed", "tracker_rows", "sba_sha256"],
               [{"run_id": RUN_ID, "sba_source_rows": sba_stats["source_data_rows"], "matching_loans": len(loans),
                 "business_groups": len(groups), "new_ids": len(new_ids),
                 "ready": coverage["eligibility_status"].get("ready", 0), "review": coverage["eligibility_status"].get("review", 0),
                 "closed": coverage["eligibility_status"].get("closed", 0), "tracker_rows": len(rows), "sba_sha256": sba_stats["sha256"]}])
    render_results(rows, coverage)
    log(f"build done: {len(groups)} businesses, {len(new_ids)} new IDs, eligibility={coverage['eligibility_status']}")
    return coverage


# --------------------------------------------------------------------------
# results page
# --------------------------------------------------------------------------
def esc(x):
    return html.escape(str(x if x is not None else ""))


def link(url, text):
    return f'<a href="{esc(url)}" target="_blank" rel="noopener">{esc(text)}</a>'


def render_results(rows, cov):
    cur = [r for r in rows if r.get("in_current_source") == "yes"]
    ready = [r for r in cur if r["eligibility_status"] == "ready"]
    conf_rank = {"high": 0, "medium": 1}
    ready.sort(key=lambda r: (conf_rank.get(r["tx_match_confidence"], 2), r["pension_status"] != "filing found",
                              -int(r["record_age_years"]), r["biz_id"]))
    show = ready[:5]
    _, plans = read_csv(P["plans"])
    plans_by = defaultdict(list)
    for p in plans:
        if not p["match_confidence"].startswith("low"):
            plans_by[p["biz_id"]].append(p)
    _, unav = read_csv(P["unavailable"])
    sba, tx, pen = cov["sba"], cov["texas"], cov["pension"]
    sba_url = sba["source_url"]
    tx_url = CFG["sources"]["TX_FRANCHISE_9CIR_EFMM"]["record_url"]
    cards = []
    for r in show:
        pl = plans_by.get(r["biz_id"], [])
        plan_html = "".join(
            f"<li>Plan {esc(p['plan_id'])} &ldquo;{esc(p['plan_name'])}&rdquo; &middot; plan year {esc(p['plan_year_begin'])} to {esc(p['plan_year_end'])}"
            f" &middot; filed {esc(p['filing_date_received'])} &middot; {esc(p['active_participants_eoy'] or 'blank')} active participants (end of plan year)"
            f" &middot; {esc(p['match_confidence'])} match: {esc(p['match_evidence'])}"
            f" &middot; {link(p['source_url'], 'DOL ' + p['form'] + ' ' + p['dataset_year'] + ' file')} (ACK {esc(p['ack_id'])})</li>" for p in pl) \
            or "<li>No matched retirement-plan filing: <b>unknown</b> (not an exclusion)</li>"
        cards.append(f"""
<div class="card">
 <h3>{esc(r['biz_id'])} &middot; {esc(r['legal_name'])}</h3>
 <p class="muted">{esc(r['address'])}, {esc(r['city'])}, TX {esc(r['zip5'])}{(' &middot; DBA ' + esc(r['dba_names'])) if r['dba_names'] else ''}</p>
 <table>
  <tr><th>PPP loans</th><td>{esc(r['draws'])}<br>{esc(r['loan_amounts_current'])}<br>{link(sba_url, 'SBA $150K+ CSV')} (search LoanNumber)</td></tr>
  <tr><th>Texas record</th><td>{link(tx_url.format(id=r['tx_taxpayer_number']), r['tx_taxpayer_name'] + ' #' + r['tx_taxpayer_number'])}<br>
     {esc(r['tx_match_confidence'])} match: {esc(r['tx_match_evidence'])}</td></tr>
  <tr><th>Record date</th><td><b>{esc(r['record_date'])}</b> ({esc(r['record_age_years'])} years) &middot; {esc(r['record_date_meaning'])}</td></tr>
  <tr><th>Historical JobsReported</th><td>{esc(r['jobs_reported_by_loan'])}</td></tr>
  <tr><th>Payroll proxy (historical)</th><td>{esc(r['historical_payroll_proxy_annual'])} &middot; {esc(r['payroll_proxy_basis'])}</td></tr>
  <tr><th>Retirement plans</th><td><ul>{plan_html}</ul></td></tr>
 </table>
</div>""")
    def tbl(d):
        return "".join(f"<tr><td>{esc(k or '(blank)')}</td><td class='n'>{v:,}</td></tr>" for k, v in sorted(d.items(), key=lambda x: -x[1]))
    unknowns = {
        "Record date unknown (no verified Texas charter date)": sum(1 for r in cur if r["record_age_30plus"] == "unknown"),
        "No matched retirement-plan filing (pension unknown)": sum(1 for r in cur if r["pension_status"].startswith("unknown")),
        "Historical payroll proxy unknown": sum(1 for r in cur if r["historical_payroll_proxy_annual"] == "unknown"),
        "JobsReported blank on at least one loan": sum(1 for r in cur if "blank" in r["jobs_reported_by_loan"]),
        "Revenue / purchase price": "never inferred",
        "Owner age / sale intent": "not established by any source",
    }
    unk_html = "".join(f"<tr><td>{esc(k)}</td><td class='n'>{esc(f'{v:,}' if isinstance(v, int) else v)}</td></tr>" for k, v in unknowns.items())
    pfiles = "".join(f"<tr><td>{esc(k)}</td><td class='n'>{v.get('data_rows', 0):,}</td><td class='n'>{v.get('candidate_rows', 0):,}</td>"
                     f"<td>{esc(v.get('encoding_detected'))}</td><td><code>{esc((v.get('sha256') or '')[:16])}</code></td></tr>"
                     for k, v in pen.get("files", {}).items())
    unav_html = "".join(f"<li><b>{esc(u['name'])}</b>: {esc(u['reason'])} {link(u['checked_url'], 'checked')}</li>" for u in unav)
    page = f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Off-market scout: TX plumbing/HVAC (NAICS 238220), PPP &ge; $150K</title>
<style>
body{{font:15px/1.5 system-ui,-apple-system,Segoe UI,Roboto,sans-serif;margin:0;background:#f5f6f8;color:#1d2330}}
main{{max-width:1100px;margin:auto;padding:24px}} h1{{font-size:24px;margin:0 0 4px}} h2{{margin-top:32px;font-size:19px}}
.muted{{color:#5d6675}} .grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px}}
.stat{{background:#fff;border-radius:10px;padding:14px;box-shadow:0 1px 2px #0001}} .stat b{{display:block;font-size:24px}}
.card{{background:#fff;border-radius:10px;padding:16px 18px;margin:14px 0;box-shadow:0 1px 2px #0001}} .card h3{{margin:0}}
table{{border-collapse:collapse;width:100%}} th,td{{text-align:left;vertical-align:top;padding:6px 8px;border-bottom:1px solid #eceef2}}
th{{width:190px;color:#5d6675;font-weight:600}} td.n{{text-align:right;font-variant-numeric:tabular-nums}}
.two{{display:grid;grid-template-columns:1fr 1fr;gap:16px}} .box{{background:#fff;border-radius:10px;padding:12px 14px;box-shadow:0 1px 2px #0001}}
.warn{{background:#fff8e6;border-left:4px solid #e5a400;padding:10px 14px;border-radius:6px}} code{{font-size:12px}} ul{{margin:4px 0;padding-left:18px}}
a{{color:#0b5bd3}} @media(max-width:760px){{.two{{grid-template-columns:1fr}}}}
</style></head><body><main>
<h1>Off-market scout: Texas plumbing &amp; HVAC contractors</h1>
<p class="muted">NAICS 238220 &middot; BorrowerState=TX &middot; PPP CurrentApprovalAmount &ge; $150,000 &middot; run {esc(cov['run_id'])} &middot; ages as of {esc(cov['as_of_date'])}</p>
<div class="warn">Record age is the age of a Texas registration record. It does not establish owner age or intent to sell.
Retirement-plan participants are not current company headcount. JobsReported is historical (2020&ndash;2021). Revenue and purchase price are never inferred.</div>

<h2>Stage counts</h2>
<div class="grid">
 <div class="stat"><b>{sba['source_data_rows']:,}</b>SBA source rows</div>
 <div class="stat"><b>{sba['rows_borrower_state_tx']:,}</b>BorrowerState = TX</div>
 <div class="stat"><b>{sba['rows_tx_naics_238220']:,}</b>&hellip; and NAICS 238220</div>
 <div class="stat"><b>{sba['matching_unique_loans']:,}</b>matching loans (&ge; $150K)</div>
 <div class="stat"><b>{cov['business_groups']:,}</b>business groups</div>
 <div class="stat"><b>{cov['eligibility_status'].get('ready', 0):,}</b>ready</div>
 <div class="stat"><b>{cov['eligibility_status'].get('review', 0):,}</b>review</div>
 <div class="stat"><b>{cov['eligibility_status'].get('closed', 0):,}</b>closed</div>
</div>
<div class="two" style="margin-top:16px">
 <div class="box"><b>Texas registration join</b><table>{tbl(cov['texas_match_status'])}</table>
   <b>Record aged 30+ years</b><table>{tbl(cov['record_age_30plus'])}</table></div>
 <div class="box"><b>Retirement-plan filings</b><table>{tbl(cov['pension_status'])}</table>
   <b>Review queue rows by stage</b><table>{tbl(cov['review_queue_by_stage'])}</table></div>
</div>

<h2>Five ready businesses</h2>
<p class="muted">{len(ready):,} businesses are <b>ready</b> (verified Texas record dated 30+ years ago). Shown: highest-confidence matches first, then those with a matched retirement-plan filing.</p>
{''.join(cards) or '<p>No ready businesses in this run.</p>'}

<h2>Unknowns</h2>
<div class="box"><table>{unk_html}</table></div>

<h2>Source coverage</h2>
<div class="box"><table>
<tr><th>SBA PPP $150K+</th><td>{link(sba_url, os.path.basename(sba_url))} &middot; data as of {esc(sba['data_as_of'])} &middot; {sba['bytes']:,} bytes &middot; sha256 <code>{esc(sba['sha256'])}</code><br>
 encoding: {esc(sba['encoding_detected'])} ({sba['lines_decoded_cp1252_fallback']:,} cp1252-fallback lines) &middot; {sba['physical_lines']:,} physical lines &middot; {sba['source_data_rows']:,} data rows &middot;
 {sba.get('rows_wrong_field_count', 0):,} malformed rows &middot; {sba['duplicate_loan_numbers_in_matches']} duplicate loan numbers &middot;
 {sba['excluded_blank_borrowerstate_but_projectstate_tx']} loans with blank BorrowerState but ProjectState=TX (excluded by the BorrowerState rule, listed in coverage_report.json)</td></tr>
<tr><th>Texas franchise (9cir-efmm)</th><td>{link(CFG['sources']['TX_FRANCHISE_9CIR_EFMM']['dataset_page'], 'Active Franchise Taxpayers')} &middot; {tx.get('data_rows', 0):,} rows &middot; dataset updated {esc(tx.get('dataset_rows_updated_at', '')[:10])} &middot; sha256 <code>{esc(tx.get('sha256', ''))}</code></td></tr>
<tr><th>DOL Form 5500 / 5500-SF</th><td>plan years {esc(pen.get('plan_years'))}, all sponsor states &middot; EINs known from Texas taxpayer numbers: {pen.get('eins_known_from_texas', 0):,}
 <table><tr><th>file</th><th>rows</th><th>candidates</th><th>encoding</th><th>sha256</th></tr>{pfiles}</table></td></tr>
</table></div>

<h2>Sources logged as unavailable</h2>
<div class="box"><ul>{unav_html}</ul></div>
<p class="muted">Files: {link('scout-tracker.csv', 'scout-tracker.csv')} &middot; {link('data/source_ledger.csv', 'source ledger')} &middot;
{link('data/review_queue.csv', 'review queue')} &middot; {link('data/loans.csv', 'loans')} &middot; {link('data/pension_plans.csv', 'pension plans')} &middot;
{link('data/coverage_report.json', 'coverage report')} &middot; {link('data/download_log.csv', 'download log')} &middot; {link('SCOUT.md', 'runbook')}</p>
</main></body></html>"""
    open(RESULTS, "w", encoding="utf-8").write(page)


# --------------------------------------------------------------------------
# verify
# --------------------------------------------------------------------------
def cmd_verify():
    checks = []

    def check(name, ok, detail=""):
        checks.append({"check": name, "ok": bool(ok), "detail": detail})
        log(f"{'PASS' if ok else 'FAIL'} {name} {detail}")

    tf, tracker = read_csv(TRACKER)
    _, loans = read_csv(P["loans"])
    _, ledger = read_csv(P["ledger"])
    _, reg = read_csv(P["registry"])
    cov = json.load(open(P["coverage"]))
    ids = [r["biz_id"] for r in tracker]
    check("unique biz_id in tracker", len(ids) == len(set(ids)), f"{len(ids)} rows")
    check("biz_id format", all(re.fullmatch(re.escape(CFG["id_prefix"]) + r"\d{%d,}" % CFG["id_width"], i) for i in ids))
    lns = [r["loan_number"] for r in loans]
    check("unique loan numbers", len(lns) == len(set(lns)), f"{len(lns)} loans")
    cur_loans = [r for r in loans if r["in_current_source"] == "yes"]
    check("every matching source loan stored once", len(cur_loans) == cov["sba"]["matching_unique_loans"],
          f"{len(cur_loans)} vs {cov['sba']['matching_unique_loans']}")
    check("matching rows = unique loans + duplicates",
          cov["sba"]["rows_tx_naics_amount_ge_150k"] == cov["sba"]["matching_unique_loans"] + cov["sba"]["duplicate_loan_numbers_in_matches"])
    check("NAICS rows = >=150K + <150K",
          cov["sba"]["rows_tx_naics_238220"] == cov["sba"]["rows_tx_naics_amount_ge_150k"] + cov["sba"].get("rows_tx_naics_amount_lt_150k", 0))
    tr_ids = set(ids)
    check("every loan maps to a tracker business", all(r["biz_id"] in tr_ids for r in loans))
    loan_count = Counter(r["biz_id"] for r in cur_loans)
    cur_biz = [r for r in tracker if r["in_current_source"] == "yes"]
    check("tracker loan_count equals loans.csv", all(int(r["loan_count"]) == loan_count[r["biz_id"]] for r in cur_biz))
    check("business groups = current tracker rows", len(cur_biz) == cov["business_groups"], f"{len(cur_biz)}")
    in_tracker_loans = [x for r in cur_biz for x in r["loan_numbers"].split(";")]
    check("no loan assigned to two businesses", len(in_tracker_loans) == len(set(in_tracker_loans)))
    keys = [(r["biz_id"], r["source_id"], r["source_record_id"]) for r in ledger]
    check("ledger has no duplicate evidence rows", len(keys) == len(set(keys)), f"{len(keys)} rows")
    check("every current loan has a ledger row",
          {r["loan_number"] for r in cur_loans} <= {r["source_record_id"] for r in ledger if r["source_id"] == "SBA_PPP_150K_PLUS"})
    check("ledger rows have URL + retrieval date + sha256", all(r["url"] and r["retrieved_date"] and r["source_sha256"] for r in ledger))
    check("eligibility values valid", all(r["eligibility_status"] in ("ready", "review", "closed") for r in cur_biz))
    check("ready => verified TX + 30+ years",
          all(r["tx_match_status"] == "verified" and r["record_age_30plus"] == "yes" for r in cur_biz if r["eligibility_status"] == "ready"))
    check("verified TX joins have name + address/ZIP corroboration",
          all(r["tx_match_confidence"] in ("high", "medium") and ("corroborat" in r["tx_match_evidence"]) for r in cur_biz if r["tx_match_status"] == "verified"))
    check("payroll proxy only on corporations with documented basis",
          all(r["historical_payroll_proxy_annual"] == "unknown" or "/ 2.5 x 12" in r["payroll_proxy_basis"] for r in cur_biz))
    _, plans = read_csv(P["plans"])
    check("pension plans carry retirement benefit codes", all(re.search(r"\b[12][A-Z]\b", p["retirement_codes"]) for p in plans))
    pid = [(p["biz_id"], p["plan_id"]) for p in plans if not p["match_confidence"].startswith("low")]
    check("one row per business+plan (latest filing only)", len(pid) == len(set(pid)))
    reg_active = [r["biz_id"] for r in reg if r["status"] == "active"]
    check("registry IDs never reused", len([r["biz_id"] for r in reg]) == len({r["biz_id"] for r in reg}))
    check("every tracker ID in registry", tr_ids <= set(reg_active))
    check("outreach_status column present", "outreach_status" in tf)
    out = {"run_id": RUN_ID, "passed": all(c["ok"] for c in checks), "checks": checks}
    json.dump(out, open(P["verification"], "w"), indent=1)
    if not out["passed"]:
        raise SystemExit("verification failed; see data/verification.json")
    return out


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "all"
    force = "--force" in sys.argv
    if cmd == "fetch":
        cmd_fetch(force)
    elif cmd == "build":
        cmd_build()
    elif cmd == "verify":
        cmd_verify()
    elif cmd == "all":
        cmd_fetch(force)
        cmd_build()
        cmd_verify()
    else:
        print(__doc__)
        sys.exit(2)


if __name__ == "__main__":
    main()
