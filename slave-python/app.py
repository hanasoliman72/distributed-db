# slave-python/app.py
#
# Python slave node (Shard B).
# Security: every request must carry X-Gateway-Token (HMAC-SHA256).
# Fault tolerance: writes go to both primary and <db>_replica schema.
#                  reads fall back to replica on primary failure.

import hashlib, hmac, time, logging, traceback, os, re
from flask import Flask, request, jsonify
import mysql.connector

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("slave-python")

# ── Config ────────────────────────────────────────────────────────────────
MYSQL_CFG = {
    "host":        os.getenv("MYSQL_HOST", "127.0.0.1"),
    "port":        int(os.getenv("MYSQL_PORT", "3306")),
    "user":        os.getenv("MYSQL_USER", "root"),
    "password":    os.getenv("MYSQL_PASSWORD", "root"),
}

SHARED_SECRET = os.getenv("SLAVE_SHARED_SECRET", "Hana-1234").encode()

# ── HMAC Auth ─────────────────────────────────────────────────────────────

def verify_token(token: str) -> bool:
    try:
        parts = token.split("|", 2)
        if len(parts) != 3:
            return False
        ts, nonce, got_sig = parts
       
        msg = f"{ts}|{nonce}".encode()
        want_sig = hmac.new(SHARED_SECRET, msg, hashlib.sha256).hexdigest()
        return hmac.compare_digest(got_sig, want_sig)
    except Exception:
        return False

def require_token(f):
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get("X-Gateway-Token", "")
        if not token or not verify_token(token):
            return jsonify({"error": "forbidden: invalid or missing gateway token"}), 403
        return f(*args, **kwargs)
    return decorated

# ── MySQL helpers ─────────────────────────────────────────────────────────

def replica(db: str) -> str:
    return db + "_replica"

def get_conn():
    return mysql.connector.connect(**MYSQL_CFG)

def exec_write(sql_str: str, args=None):
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(sql_str, args or [])
        conn.commit()
        return cur.rowcount, cur.lastrowid, None
    except Exception as e:
        return 0, 0, str(e)
    finally:
        if conn:
            try: conn.close()
            except: pass

def exec_query(sql_str: str, args=None):
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(sql_str, args or [])
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = [{cols[i]: row[i] for i in range(len(cols))} for row in cur.fetchall()]
        return rows, None
    except Exception as e:
        return None, str(e)
    finally:
        if conn:
            try: conn.close()
            except: pass

def build_where(where: dict):
    if not where:
        return "", []
    clauses = [f"`{c}` = %s" for c in where]
    args    = [where[c] for c in where]  # Keep original types, don't stringify
    return " AND ".join(clauses), args

def is_valid_identifier(s: str) -> bool:
    """Validate SQL identifier (db/table name)"""
    if not s or len(s) > 64:
        return False
    return bool(re.match(r'^[a-zA-Z0-9_-]+$', s))

# ── Health ────────────────────────────────────────────────────────────────

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "role": "slave-python"})

# ── DB DDL ────────────────────────────────────────────────────────────────

@app.route("/shard/db/create", methods=["POST"])
@require_token
def create_db():
    data = request.get_json()
    db = data.get("db", "")
    if not db or not is_valid_identifier(db):
        return jsonify({"error": "invalid db name"}), 400
    for schema in [db, replica(db)]:
        _, _, err = exec_write(f"CREATE DATABASE IF NOT EXISTS `{schema}`")
        if err: return jsonify({"error": err}), 500
    return jsonify({"status": "ok"}), 201

@app.route("/shard/db/drop", methods=["DELETE"])
@require_token
def drop_db():
    db = request.get_json().get("db")
    for schema in [db, replica(db)]:
        exec_write(f"DROP DATABASE IF EXISTS `{schema}`")
    return jsonify({"status": "ok"})

# ── Table DDL ─────────────────────────────────────────────────────────────

@app.route("/shard/table/create", methods=["POST"])
@require_token
def create_table():
    data  = request.get_json()
    db    = data.get("db", "")
    table = data.get("table", "")
    attrs = data.get("attributes", [])
    if not db or not table or not is_valid_identifier(db) or not is_valid_identifier(table):
        return jsonify({"error": "invalid db or table name"}), 400
    col_defs = ["`id` INT AUTO_INCREMENT PRIMARY KEY"]
    for a in attrs:
        if not is_valid_identifier(a):
            return jsonify({"error": f"invalid attribute name: {a}"}), 400
        if a.lower() != "id":
            col_defs.append(f"`{a}` TEXT")
    cols = ", ".join(col_defs)
    for schema in [db, replica(db)]:
        exec_write(f"CREATE DATABASE IF NOT EXISTS `{schema}`")
        _, _, err = exec_write(f"CREATE TABLE IF NOT EXISTS `{schema}`.`{table}` ({cols})")
        if err: return jsonify({"error": err}), 500
    return jsonify({"status": "ok"}), 201

@app.route("/shard/table/drop", methods=["DELETE"])
@require_token
def drop_table():
    data = request.get_json(); db = data.get("db"); table = data.get("table")
    for schema in [db, replica(db)]:
        exec_write(f"DROP TABLE IF EXISTS `{schema}`.`{table}`")
    return jsonify({"status": "ok"})

# ── INSERT ────────────────────────────────────────────────────────────────

@app.route("/shard/query/insert", methods=["POST"])
@require_token
def insert():
    data   = request.get_json()
    db     = data.get("db"); table = data.get("table")
    record = {k: v for k, v in data.get("record", {}).items() if k.lower() != "id"}
    if not db or not table or not record:
        return jsonify({"error": "db, table, record required"}), 400

    cols = [f"`{c}`" for c in record]
    phs  = ["%s"] * len(record)
    vals = [str(v) for v in record.values()]
    _, gen_id, err = exec_write(
        f"INSERT INTO `{db}`.`{table}` ({', '.join(cols)}) VALUES ({', '.join(phs)})", vals)
    if err: return jsonify({"error": err}), 500

    # Mirror to replica with explicit id.
    rep_record = dict(record)
    rep_record["id"] = gen_id
    rep_cols = [f"`{c}`" for c in rep_record]
    rep_phs  = ["%s"] * len(rep_record)
    rep_vals = [str(v) for v in rep_record.values()]
    exec_write(
        f"INSERT IGNORE INTO `{replica(db)}`.`{table}` ({', '.join(rep_cols)}) VALUES ({', '.join(rep_phs)})",
        rep_vals)

    return jsonify({"message": "record inserted", "generated_id": gen_id}), 201

# ── SELECT ────────────────────────────────────────────────────────────────

@app.route("/shard/query/select", methods=["GET"])
@require_token
def select_rows():
    args_q = request.args
    db     = args_q.get("db"); table = args_q.get("table")
    if not db or not table:
        return jsonify({"error": "db and table required"}), 400
    where = {k: v for k, v in args_q.items() if k not in ("db", "table")}
    cond, w_args = build_where(where)
    sql_str = f"SELECT * FROM `{db}`.`{table}`"
    if cond: sql_str += " WHERE " + cond

    # Try primary, fall back to replica.
    rows, err = exec_query(sql_str, w_args or None)
    if err:
        replica_sql = sql_str.replace(f"`{db}`.", f"`{replica(db)}`.", 1)
        rows, err = exec_query(replica_sql, w_args or None)
    if err: return jsonify({"error": err}), 500
    return jsonify({"count": len(rows), "records": rows})

# ── UPDATE ────────────────────────────────────────────────────────────────

@app.route("/shard/query/update", methods=["PUT"])
@require_token
def update_rows():
    data  = request.get_json()
    db    = data.get("db"); table = data.get("table")
    where = data.get("where", {}); set_ = data.get("set", {})
    if not set_: return jsonify({"error": "set required"}), 400

    set_clauses = [f"`{c}` = %s" for c in set_]
    args        = [str(v) for v in set_.values()]
    sql_str     = f"UPDATE `{db}`.`{table}` SET {', '.join(set_clauses)}"
    cond, w_args = build_where(where)
    if cond: sql_str += " WHERE " + cond; args += w_args

    affected, _, err = exec_write(sql_str, args)
    if err: return jsonify({"error": err}), 500

    # Best-effort replica update.
    rep_sql = sql_str.replace(f"`{db}`.", f"`{replica(db)}`.", 1)
    _, _, err = exec_write(rep_sql, args)
    if err:
        log.warning(f"[replica] failed to update replica {db}.{table}: {err}")

    return jsonify({"message": "update complete", "records_updated": affected})

# ── DELETE ────────────────────────────────────────────────────────────────

@app.route("/shard/query/delete", methods=["DELETE"])
@require_token
def delete_rows():
    data  = request.get_json()
    db    = data.get("db"); table = data.get("table")
    where = data.get("where", {})
    sql_str = f"DELETE FROM `{db}`.`{table}`"; args = []
    cond, w_args = build_where(where)
    if cond: sql_str += " WHERE " + cond; args = w_args

    affected, _, err = exec_write(sql_str, args)
    if err: return jsonify({"error": err}), 500

    rep_sql = sql_str.replace(f"`{db}`.", f"`{replica(db)}`.", 1)
    exec_write(rep_sql, args)

    return jsonify({"message": "delete complete", "records_deleted": affected})

# ── SEARCH ────────────────────────────────────────────────────────────────

@app.route("/shard/query/search", methods=["GET"])
@require_token
def search():
    args_q = request.args
    db     = args_q.get("db"); table = args_q.get("table"); term = args_q.get("q", "").strip()
    if not db or not table or not term:
        return jsonify({"error": "db, table, q required"}), 400
    rows, err = exec_query(f"SELECT * FROM `{db}`.`{table}`")
    if err:
        rows, err = exec_query(f"SELECT * FROM `{replica(db)}`.`{table}`")
    if err: return jsonify({"error": err}), 500

    term_lower = term.lower()
    matched = [r for r in rows if any(term_lower in str(v).lower() for v in r.values() if v is not None)]
    return jsonify({"search_term": term, "count": len(matched), "records": matched})

# ── Error handler ─────────────────────────────────────────────────────────

@app.errorhandler(Exception)
def handle_error(e):
    log.error("Unhandled:\n%s", traceback.format_exc())
    return jsonify({"error": str(e)}), 500

# ── Entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = 8082
    log.info("Python slave listening on :%d", port)
    app.run(host="0.0.0.0", port=port, debug=False)