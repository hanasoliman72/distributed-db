# slave-python/app.py  –  Python slave, port 8083

from flask import Flask, request, jsonify
import mysql.connector
import requests
import threading
import logging
import time

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("slave-python")

# ── MySQL config ──────────────────────────────────────────────────────────
MYSQL_CONFIG = {
    "host":     "127.0.0.1",
    "port":     3306,
    "user":     "root",
    "password": "root",
}

def get_conn():
    return mysql.connector.connect(**MYSQL_CONFIG)

# ── Cluster config ────────────────────────────────────────────────────────
SELF_ADDR   = "http://127.0.0.1:8083"
MASTER_ADDR = "http://127.0.0.1:8080"

PEERS = [
    MASTER_ADDR,
    "http://127.0.0.1:8081",# C# slave
    "http://127.0.0.1:8082",# Go slave
]

# ── Role state ────────────────────────────────────────────────────────────
_master_down      = False
_is_acting_master = False   # True → this slave promoted itself
_state_lock       = threading.Lock()

def set_master_down(down: bool):
    global _master_down, _is_acting_master
    with _state_lock:
        if down and not _master_down:
            log.warning("Master %s unreachable — promoting self to acting master", MASTER_ADDR)
            _is_acting_master = True
        elif not down and _master_down:
            log.info("Master %s is back online — reverting to slave role", MASTER_ADDR)
            _is_acting_master = False
        _master_down = down

def is_master_down() -> bool:
    with _state_lock:
        return _master_down

def self_role() -> str:
    with _state_lock:
        return "slave-python (acting master)" if _is_acting_master else "slave-python"

def can_manage_db() -> bool:
    """
    Create / drop DATABASE is restricted to the real master.
    When this slave promotes itself (master is down) it gains that right.
    Normal slave → False.  Acting master → True.
    """
    with _state_lock:
        return _is_acting_master

# ── Background master watcher ─────────────────────────────────────────────
def master_watcher():
    while True:
        time.sleep(5)
        try:
            r = requests.get(MASTER_ADDR + "/health", timeout=3)
            r.raise_for_status()
            set_master_down(False)
        except Exception:
            set_master_down(True)

threading.Thread(target=master_watcher, daemon=True).start()

# ── Broadcaster ───────────────────────────────────────────────────────────
def broadcast(path: str, payload: dict):
    """POST payload to every peer except ourselves. Fire-and-forget."""
    for peer in PEERS:
        if peer == SELF_ADDR:
            continue
        if peer == MASTER_ADDR and is_master_down():
            log.info("[broadcast] skipping down master %s", peer)
            continue

        def _send(url, p, body):
            try:
                requests.post(url + p, json=body, timeout=5)
                if url == MASTER_ADDR:
                    set_master_down(False)
            except Exception as e:
                log.warning("[broadcast] POST %s%s failed: %s", url, p, e)
                if url == MASTER_ADDR:
                    set_master_down(True)

        threading.Thread(target=_send, args=(peer, path, payload), daemon=True).start()

# ── Helpers ───────────────────────────────────────────────────────────────
def build_where(where: dict):
    if not where:
        return "", []
    clauses = [f"`{col}` = %s" for col in where]
    args    = [str(v) for v in where.values()]
    return " AND ".join(clauses), args

def scan_rows(cursor) -> list:
    cols = [d[0] for d in cursor.description]
    return [{cols[i]: row[i] for i in range(len(cols))} for row in cursor.fetchall()]

# ── Health ────────────────────────────────────────────────────────────────
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "role": self_role()})

# ═══════════════════════════════════════════════════════════════════════════
#  REPLICATION RECEIVERS
#  Called by master or other slaves — apply locally only, NO re-broadcast.
# ═══════════════════════════════════════════════════════════════════════════

@app.route("/replicate/db/create", methods=["POST"])
def replicate_create_db():
    data = request.get_json()
    db   = data.get("db")
    if not db:
        return jsonify({"error": "'db' is required"}), 400
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS `{db}`")
        conn.commit()
    finally:
        cur.close(); conn.close()
    return jsonify({"status": "replicated"})

@app.route("/replicate/db/drop", methods=["POST"])
def replicate_drop_db():
    data = request.get_json()
    db   = data.get("db")
    if not db:
        return jsonify({"error": "'db' is required"}), 400
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(f"DROP DATABASE IF EXISTS `{db}`")
        conn.commit()
    finally:
        cur.close(); conn.close()
    return jsonify({"status": "replicated"})

@app.route("/replicate/table/create", methods=["POST"])
def replicate_create_table():
    data  = request.get_json()
    db    = data.get("db"); table = data.get("table"); attrs = data.get("attributes", [])
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS `{db}`")
        col_defs = ["`id` INT AUTO_INCREMENT PRIMARY KEY"]
        for a in attrs:
            if a.lower() != "id":
                col_defs.append(f"`{a}` TEXT")
        cur.execute(f"CREATE TABLE IF NOT EXISTS `{db}`.`{table}` ({', '.join(col_defs)})")
        conn.commit()
    finally:
        cur.close(); conn.close()
    return jsonify({"status": "replicated"})

@app.route("/replicate/table/drop", methods=["POST"])
def replicate_drop_table():
    data  = request.get_json()
    db    = data.get("db"); table = data.get("table")
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(f"DROP TABLE IF EXISTS `{db}`.`{table}`")
        conn.commit()
    finally:
        cur.close(); conn.close()
    return jsonify({"status": "replicated"})

@app.route("/replicate/query/insert", methods=["POST"])
def replicate_insert():
    data   = request.get_json()
    db     = data.get("db"); table = data.get("table"); record = data.get("record", {})
    cols   = [f"`{c}`" for c in record]
    phs    = ["%s"     for _ in record]
    vals   = [str(v)   for v in record.values()]
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(f"INSERT INTO `{db}`.`{table}` ({', '.join(cols)}) VALUES ({', '.join(phs)})", vals)
        conn.commit()
    finally:
        cur.close(); conn.close()
    return jsonify({"status": "replicated"})

@app.route("/replicate/query/update", methods=["POST"])
def replicate_update():
    data  = request.get_json()
    db    = data.get("db"); table = data.get("table")
    where = data.get("where", {}); set_ = data.get("set", {})
    set_clauses = [f"`{c}` = %s" for c in set_]
    args = [str(v) for v in set_.values()]
    query = f"UPDATE `{db}`.`{table}` SET {', '.join(set_clauses)}"
    cond, w_args = build_where(where)
    if cond:
        query += " WHERE " + cond; args += w_args
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(query, args); conn.commit()
    finally:
        cur.close(); conn.close()
    return jsonify({"status": "replicated"})

@app.route("/replicate/query/delete", methods=["POST"])
def replicate_delete():
    data  = request.get_json()
    db    = data.get("db"); table = data.get("table"); where = data.get("where", {})
    query = f"DELETE FROM `{db}`.`{table}`"; args = []
    cond, w_args = build_where(where)
    if cond:
        query += " WHERE " + cond; args = w_args
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(query, args); conn.commit()
    finally:
        cur.close(); conn.close()
    return jsonify({"status": "replicated"})

@app.route("/replicate/snapshot", methods=["POST"])
def replicate_snapshot():
    snapshot  = request.get_json()
    databases = snapshot.get("databases", {})
    conn = get_conn(); cur = conn.cursor()
    try:
        for db_name, tables in databases.items():
            cur.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")
            for tbl_name, tbl in tables.items():
                attrs   = tbl.get("attributes", [])
                records = tbl.get("records", [])
                cur.execute(f"DROP TABLE IF EXISTS `{db_name}`.`{tbl_name}`")
                col_defs = ["`id` INT AUTO_INCREMENT PRIMARY KEY"]
                for a in attrs:
                    if a.lower() != "id":
                        col_defs.append(f"`{a}` TEXT")
                cur.execute(f"CREATE TABLE IF NOT EXISTS `{db_name}`.`{tbl_name}` ({', '.join(col_defs)})")
                for rec in records:
                    cols = [f"`{c}`" for c in rec]
                    phs  = ["%s"     for _ in rec]
                    vals = [str(v)   for v in rec.values()]
                    cur.execute(f"INSERT INTO `{db_name}`.`{tbl_name}` ({', '.join(cols)}) VALUES ({', '.join(phs)})", vals)
        conn.commit()
    finally:
        cur.close(); conn.close()
    return jsonify({"status": "snapshot applied"})

# ═══════════════════════════════════════════════════════════════════════════
#  CLIENT-FACING ENDPOINTS
#
#  ALLOWED for normal slaves:
#    table create/drop, row insert/update/delete/select
#
#  RESTRICTED to acting master only (master is down):
#    database create/drop
#
#  Each write endpoint: apply locally → broadcast to all peers.
# ═══════════════════════════════════════════════════════════════════════════

# ── Database (master-only, unlocks when acting as master) ─────────────────

@app.route("/query/db/create", methods=["POST"])
def query_create_db():
    if not can_manage_db():
        return jsonify({
            "error": "database create/drop is only allowed on the master node",
            "tip":   "send this request to the master at " + MASTER_ADDR
        }), 403

    data = request.get_json()
    db   = data.get("db")
    if not db:
        return jsonify({"error": "'db' is required"}), 400

    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS `{db}`")
        conn.commit()
    except mysql.connector.Error as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close(); conn.close()

    broadcast("/replicate/db/create", {"db": db})
    return jsonify({"message": f"database '{db}' created", "served_by": self_role()}), 201


@app.route("/query/db/drop", methods=["DELETE"])
def query_drop_db():
    if not can_manage_db():
        return jsonify({
            "error": "database create/drop is only allowed on the master node",
            "tip":   "send this request to the master at " + MASTER_ADDR
        }), 403

    data  = request.get_json()
    db    = data.get("db")
    if not db:
        return jsonify({"error": "'db' is required"}), 400

    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(f"DROP DATABASE IF EXISTS `{db}`")
        conn.commit()
    except mysql.connector.Error as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close(); conn.close()

    broadcast("/replicate/db/drop", {"db": db})
    return jsonify({"message": f"database '{db}' dropped", "served_by": self_role()})

# ── Tables (always allowed on slaves) ────────────────────────────────────

@app.route("/query/table/create", methods=["POST"])
def query_create_table():
    if not can_manage_db():
        return jsonify({
            "error": "database create/drop is only allowed on the master node",
            "tip":   "send this request to the master at " + MASTER_ADDR
        }), 403
    
    data  = request.get_json()
    db    = data.get("db")
    table = data.get("table")
    attrs = data.get("attributes", [])
    if not db or not table or not attrs:
        return jsonify({"error": "'db', 'table', and 'attributes' are required"}), 400

    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS `{db}`")
        col_defs = ["`id` INT AUTO_INCREMENT PRIMARY KEY"]
        for a in attrs:
            if a.lower() != "id":
                col_defs.append(f"`{a}` TEXT")
        cur.execute(f"CREATE TABLE IF NOT EXISTS `{db}`.`{table}` ({', '.join(col_defs)})")
        conn.commit()
    except mysql.connector.Error as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close(); conn.close()

    broadcast("/replicate/table/create", {"db": db, "table": table, "attributes": attrs})
    return jsonify({"message": f"table '{table}' created", "served_by": self_role()}), 201


@app.route("/query/table/drop", methods=["DELETE"])
def query_drop_table():
    data  = request.get_json()
    db    = data.get("db")
    table = data.get("table")
    if not db or not table:
        return jsonify({"error": "'db' and 'table' are required"}), 400

    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(f"DROP TABLE IF EXISTS `{db}`.`{table}`")
        conn.commit()
    except mysql.connector.Error as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close(); conn.close()

    broadcast("/replicate/table/drop", {"db": db, "table": table})
    return jsonify({"message": f"table '{table}' dropped", "served_by": self_role()})

# ── Rows (always allowed on slaves) ──────────────────────────────────────

@app.route("/query/select", methods=["GET"])
def local_select():
    args_q = request.args
    db = args_q.get("db"); table = args_q.get("table")
    if not db or not table:
        return jsonify({"error": "'db' and 'table' are required"}), 400
    where = {k: v for k, v in args_q.items() if k not in ("db", "table")}
    query = f"SELECT * FROM `{db}`.`{table}`"; args = []
    cond, w_args = build_where(where)
    if cond:
        query += " WHERE " + cond; args = w_args
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(query, args); records = scan_rows(cur)
    finally:
        cur.close(); conn.close()
    return jsonify({"count": len(records), "records": records, "served_by": self_role() + " :8083"})


@app.route("/query/insert", methods=["POST"])
def local_insert():
    data   = request.get_json()
    db     = data.get("db")
    table  = data.get("table")
    record = data.get("record", {})
    if not db or not table or not record:
        return jsonify({"error": "'db', 'table', and 'record' are required"}), 400

    record = {k: v for k, v in record.items() if k.lower() != "id"}
    cols = [f"`{c}`" for c in record]
    phs  = ["%s"     for _ in record]
    vals = [str(v)   for v in record.values()]

    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            f"INSERT INTO `{db}`.`{table}` ({', '.join(cols)}) VALUES ({', '.join(phs)})",
            vals
        )
        conn.commit()
        generated_id = cur.lastrowid
    finally:
        cur.close(); conn.close()

    broadcast_record = dict(record)
    broadcast_record["id"] = generated_id
    broadcast("/replicate/query/insert", {"db": db, "table": table, "record": broadcast_record})

    return jsonify({"message": "record inserted", "generated_id": generated_id, "served_by": self_role() + " :8083"}), 201


@app.route("/query/update", methods=["PUT"])
def local_update():
    data  = request.get_json()
    db    = data.get("db")
    table = data.get("table")
    where = data.get("where", {})
    set_  = data.get("set", {})
    if not db or not table or not set_:
        return jsonify({"error": "'db', 'table', 'where', and 'set' are required"}), 400

    set_clauses = [f"`{c}` = %s" for c in set_]
    args        = [str(v) for v in set_.values()]
    query = f"UPDATE `{db}`.`{table}` SET {', '.join(set_clauses)}"
    cond, w_args = build_where(where)
    if cond:
        query += " WHERE " + cond; args += w_args

    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(query, args); conn.commit(); affected = cur.rowcount
    finally:
        cur.close(); conn.close()

    broadcast("/replicate/query/update", {"db": db, "table": table, "where": where, "set": set_})
    return jsonify({"message": "update complete", "records_updated": affected, "served_by": self_role() + " :8083"})


@app.route("/query/delete", methods=["DELETE"])
def local_delete():
    data  = request.get_json()
    db    = data.get("db")
    table = data.get("table")
    where = data.get("where", {})
    if not db or not table:
        return jsonify({"error": "'db', 'table', and 'where' are required"}), 400

    query = f"DELETE FROM `{db}`.`{table}`"; args = []
    cond, w_args = build_where(where)
    if cond:
        query += " WHERE " + cond; args = w_args

    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(query, args); conn.commit(); affected = cur.rowcount
    finally:
        cur.close(); conn.close()

    broadcast("/replicate/query/delete", {"db": db, "table": table, "where": where})
    return jsonify({"message": "delete complete", "records_deleted": affected, "served_by": self_role() + " :8083"})

# ═══════════════════════════════════════════════════════════════════════════
#  UNIQUE PYTHON FEATURE: full-text search across ALL columns
#  GET /query/search?db=mydb&table=users&q=ali
# ═══════════════════════════════════════════════════════════════════════════

@app.route("/query/search", methods=["GET"])
def full_text_search():
    args_q = request.args
    db     = args_q.get("db"); table = args_q.get("table"); term = args_q.get("q", "").strip()
    if not db or not table:
        return jsonify({"error": "'db' and 'table' are required"}), 400
    if not term:
        return jsonify({"error": "'q' (search term) is required"}), 400
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(f"SELECT * FROM `{db}`.`{table}`")
        all_rows = scan_rows(cur)
    finally:
        cur.close(); conn.close()
    term_lower = term.lower()
    matched = [row for row in all_rows
               if any(term_lower in str(v).lower() for v in row.values() if v is not None)]
    return jsonify({"search_term": term, "count": len(matched), "records": matched,
                    "served_by": self_role() + " :3 (full-text search)"})

# ── Entry point ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("Python slave listening on :8083")
    app.run(host="0.0.0.0", port=8083, debug=False)