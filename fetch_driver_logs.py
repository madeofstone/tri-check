#!/usr/bin/env python3
"""
Pull Databricks driver logs (log4j / stderr / stdout) for an AAC job run and
extract the root exception behind a Spark failure.

Uses DATABRICKS_HOST / DATABRICKS_TOKEN from .env (same as tri-track).
Standard library only.

Usage:
    python3 fetch_driver_logs.py --run-id 152368479367127
    python3 fetch_driver_logs.py --cluster-id 0922-183035-abcd1234
Output lands in jobs/<run_or_cluster>/driver_logs/ plus a findings.txt summary.
"""
import argparse, base64, gzip, json, os, re, sys, urllib.parse, urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent

def load_env():
    env = {}
    for line in (HERE / ".env").read_text().splitlines():
        line = line.strip()
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip().strip("'\"")
    return env

ENV = load_env()
HOST = ENV["DATABRICKS_HOST"].rstrip("/")
TOKEN = ENV["DATABRICKS_TOKEN"]

def api(endpoint, **q):
    url = HOST + endpoint + ("?" + urllib.parse.urlencode(q) if q else "")
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {TOKEN}"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)

def cluster_from_run(run_id):
    r = api("/api/2.1/jobs/runs/get", run_id=run_id)
    print(f"job_id={r.get('job_id')} run_name={r.get('run_name')} state={r.get('state')}")
    cid = (r.get("cluster_instance") or {}).get("cluster_id")
    for t in r.get("tasks", []):
        cid = cid or (t.get("cluster_instance") or {}).get("cluster_id")
    conf = None
    for t in r.get("tasks", []) + [r]:
        nc = (t.get("new_cluster") or (t.get("cluster_spec") or {}).get("new_cluster") or {})
        conf = conf or nc.get("cluster_log_conf")
    print(f"cluster_id={cid}  cluster_log_conf={json.dumps(conf)}")
    return cid, conf

def dbfs_list(path):
    try:
        return api("/api/2.0/dbfs/list", path=path).get("files", [])
    except urllib.error.HTTPError as e:
        print(f"  (cannot list {path}: {e.code})")
        return []

def walk(path, depth=0, out=None):
    out = [] if out is None else out
    if depth > 5:
        return out
    for f in dbfs_list(path):
        print("  " * depth + f"{f['path']}{'/' if f['is_dir'] else ''}  {f.get('file_size','')}")
        if f["is_dir"]:
            if "/eventlog" in f["path"]:
                continue  # tri-track already handles event logs
            walk(f["path"], depth + 1, out)
        else:
            out.append(f)
    return out

def dbfs_download(path, dest):
    dest.parent.mkdir(parents=True, exist_ok=True)
    off, chunk = 0, 1024 * 1024
    with open(dest, "wb") as fh:
        while True:
            r = api("/api/2.0/dbfs/read", path=path, offset=off, length=chunk)
            n = r.get("bytes_read", 0)
            if n == 0:
                break
            fh.write(base64.b64decode(r["data"]))
            off += n
    if dest.suffix == ".gz":
        plain = dest.with_suffix("")
        with gzip.open(dest, "rb") as i, open(plain, "wb") as o:
            o.write(i.read())
        try:
            dest.unlink()
        except OSError:
            pass
        return plain
    return dest

PATTERNS = re.compile(
    r"Multiple failures in stage materialization|Caused by:|Suppressed:|Exception|Error:|"
    r"SPARK_JOB_CANCELLED|OutOfMemory|Broadcast|TransformerException|trifacta", re.I)

def extract(files, out_file):
    lines_out = []
    for f in files:
        text = f.read_text(errors="replace").splitlines()
        hits = [i for i, l in enumerate(text) if "stage materialization" in l]
        lines_out.append(f"===== {f.name}: {len(hits)} 'stage materialization' hit(s)")
        for i in hits[:3]:
            lines_out.append(f"--- {f.name} line {i+1} (context)")
            lines_out.extend(text[max(0, i - 40): i + 200])
        # also collect distinct exception headlines across the whole file
        heads = {}
        for l in text:
            m = re.search(r"((?:[\w$]+\.)+\w*(?:Exception|Error))(: .{0,250})?", l)
            if m and ("Caused by" in l or "Suppressed" in l or " ERROR " in l or l.startswith(m.group(1))):
                heads[m.group(0)[:300]] = heads.get(m.group(0)[:300], 0) + 1
        if heads:
            lines_out.append(f"--- distinct exception headlines in {f.name}")
            for h, c in sorted(heads.items(), key=lambda x: -x[1])[:60]:
                lines_out.append(f"{c:5d}  {h}")
    out_file.write_text("\n".join(lines_out))
    print(f"\nFindings written to {out_file}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", type=int)
    ap.add_argument("--cluster-id")
    ap.add_argument("--root", default="/trifacta/logs", help="DBFS log root (default /trifacta/logs)")
    a = ap.parse_args()
    if not (a.run_id or a.cluster_id):
        ap.error("pass --run-id or --cluster-id")
    cid, conf = (a.cluster_id, None) if a.cluster_id else cluster_from_run(a.run_id)
    root = a.root
    if conf and conf.get("dbfs", {}).get("destination"):
        root = conf["dbfs"]["destination"].replace("dbfs:", "").rstrip("/")
    base = f"{root}/{cid}"
    print(f"\nListing {base}")
    files = walk(base)
    wanted = [f for f in files if re.search(r"(log4j|stderr|stdout|stacktrace)", Path(f["path"]).name)
              and "/driver" in f["path"]] or \
             [f for f in files if re.search(r"(log4j|stderr|stdout|stacktrace)", Path(f["path"]).name)]
    if not wanted:
        print("No driver log4j/stderr/stdout files found under that path. "
              "Check the cluster's cluster_log_conf printed above.")
        sys.exit(1)
    outdir = HERE / "jobs" / str(a.run_id or cid) / "driver_logs"
    local = []
    for f in wanted:
        rel = f["path"][len(base):].lstrip("/")
        print(f"downloading {f['path']} ({f.get('file_size')} bytes)")
        local.append(dbfs_download(f["path"], outdir / rel))
    extract(local, outdir.parent / "findings.txt")

if __name__ == "__main__":
    main()
