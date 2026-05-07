# slave-python/app.py
# Python slave node – runs on port 8082
# Connects to MySQL, accepts replication from the Go master,
# and offers a special full-text search endpoint.

from flask import Flask, request, jsonify
import mysql.connector
import os

app = Flask(__name__)

# ── MySQL config ──────────────────────────────────────────────────────────
# Change these to match your MySQL setup (same server the master uses).
MYSQL_CONFIG = {
    "host":     "127.0.0.1",
    "port":     3306,
    "user":     "root",
    "password": "rootroot",        # ← change if your password is different
}

def get_conn():
    """Return a fresh MySQL connection (no persistent pool needed for a slave)."""
    return mysql.connector.connect(**MYSQL_CONFIG)


# ── Helpers ───────────────────────────────────────────────────────────────

def build_where(where: dict):
    """Turn {'name':'Ali','age':'20'} into ('`name`=? AND `age`=?', ['Ali','20'])."""
    if not where:
        return "", []
    clauses = [f"`{col}` = %s" for col in where]
    args    = [str(v) for v in where.values()]
    return " AND ".join(clauses), args


def scan_rows(cursor) -> list[dict]:
    """Convert cursor results to a list of dicts."""
    cols = [d[0] for d in cursor.description]
    rows = []
    for row in cursor.fetchall():
        rows.append({cols[i]: (row[i] if row[i] is not None else None)
                     for i in range(len(cols))})
    return rows


# ── Health ────────────────────────────────────────────────────────────────

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "role": "slave-python"})


# ── Replication endpoints (called by Go master) ───────────────────────────

@app.route("/replicate/table/create", methods=["POST"])
def replicate_create_table():
    """
    Master sends: { "db":"mydb", "table":"users", "attributes":["name","age"] }
    We create the same table with an AUTO_INCREMENT id column first.
    """
    data  = request.get_json()
    db    = data.get("db")
    table = data.get("table")
    attrs = data.get("attributes", [])

    conn = get_conn()
    cur  = conn.cursor()
    try:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS `{db}`")
        col_defs = ["`id` INT AUTO_INCREMENT PRIMARY KEY"]
        for a in attrs:
            if a.lower() == "id":
                continue
            col_defs.append(f"`{a}` TEXT")
        col_str = ", ".join(col_defs)
        cur.execute(f"CREATE TABLE IF NOT EXISTS `{db}`.`{table}` ({col_str})")
        conn.commit()
    finally:
        cur.close()
        conn.close()

    return jsonify({"status": "replicated"})


@app.route("/replicate/table/drop", methods=["POST"])
def replicate_drop_table():
    data  = request.get_json()
    db    = data.get("db")
    table = data.get("table")

    conn = get_conn()
    cur  = conn.cursor()
    try:
        cur.execute(f"DROP TABLE IF EXISTS `{db}`.`{table}`")
        conn.commit()
    finally:
        cur.close()
        conn.close()

    return jsonify({"status": "replicated"})


@app.route("/replicate/query/insert", methods=["POST"])
def replicate_insert():
    """
    Master sends the record WITH the generated id so both sides stay in sync.
    Body: { "db":"mydb", "table":"users", "record":{"id":1,"name":"Ali","age":"20"} }
    """
    data   = request.get_json()
    db     = data.get("db")
    table  = data.get("table")
    record = data.get("record", {})

    cols         = [f"`{c}`"  for c in record]
    placeholders = ["%s"       for _  in record]
    values       = [str(v)     for v  in record.values()]

    query = (f"INSERT INTO `{db}`.`{table}` ({', '.join(cols)}) "
             f"VALUES ({', '.join(placeholders)})")

    conn = get_conn()
    cur  = conn.cursor()
    try:
        cur.execute(query, values)
        conn.commit()
    finally:
        cur.close()
        conn.close()

    return jsonify({"status": "replicated"})


@app.route("/replicate/query/update", methods=["POST"])
def replicate_update():
    data  = request.get_json()
    db    = data.get("db")
    table = data.get("table")
    where = data.get("where", {})
    set_  = data.get("set",   {})

    set_clauses = [f"`{c}` = %s" for c in set_]
    args        = [str(v) for v in set_.values()]

    query = f"UPDATE `{db}`.`{table}` SET {', '.join(set_clauses)}"
    cond, where_args = build_where(where)
    if cond:
        query += " WHERE " + cond
        args  += where_args

    conn = get_conn()
    cur  = conn.cursor()
    try:
        cur.execute(query, args)
        conn.commit()
    finally:
        cur.close()
        conn.close()

    return jsonify({"status": "replicated"})


@app.route("/replicate/query/delete", methods=["POST"])
def replicate_delete():
    data  = request.get_json()
    db    = data.get("db")
    table = data.get("table")
    where = data.get("where", {})

    query = f"DELETE FROM `{db}`.`{table}`"
    args  = []
    cond, where_args = build_where(where)
    if cond:
        query += " WHERE " + cond
        args   = where_args

    conn = get_conn()
    cur  = conn.cursor()
    try:
        cur.execute(query, args)
        conn.commit()
    finally:
        cur.close()
        conn.close()

    return jsonify({"status": "replicated"})


@app.route("/replicate/snapshot", methods=["POST"])
def replicate_snapshot():
    """
    Full re-sync sent by the master when this slave comes back online.
    Body: { "databases": { "mydb": { "users": { "attributes":[...], "records":[...] } } } }
    """
    snapshot  = request.get_json()
    databases = snapshot.get("databases", {})

    conn = get_conn()
    cur  = conn.cursor()
    try:
        for db_name, tables in databases.items():
            cur.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")
            for tbl_name, tbl in tables.items():
                attrs   = tbl.get("attributes", [])
                records = tbl.get("records",    [])

                cur.execute(f"DROP TABLE IF EXISTS `{db_name}`.`{tbl_name}`")

                col_defs = ["`id` INT AUTO_INCREMENT PRIMARY KEY"]
                for a in attrs:
                    if a.lower() == "id":
                        continue
                    col_defs.append(f"`{a}` TEXT")
                col_str = ", ".join(col_defs)
                cur.execute(
                    f"CREATE TABLE IF NOT EXISTS `{db_name}`.`{tbl_name}` ({col_str})"
                )

                for rec in records:
                    cols         = [f"`{c}`" for c in rec]
                    placeholders = ["%s"      for _  in rec]
                    values       = [str(v)    for v  in rec.values()]
                    cur.execute(
                        f"INSERT INTO `{db_name}`.`{tbl_name}` "
                        f"({', '.join(cols)}) VALUES ({', '.join(placeholders)})",
                        values,
                    )
        conn.commit()
    finally:
        cur.close()
        conn.close()

    return jsonify({"status": "snapshot applied"})


# ── Standard SELECT (read-only, same as Go slave) ─────────────────────────

@app.route("/query/select", methods=["GET"])
def local_select():
    """
    GET /query/select?db=mydb&table=users
    GET /query/select?db=mydb&table=users&name=Ali
    GET /query/select?db=mydb&table=users&id=3
    Any extra query param becomes a WHERE condition (AND logic).
    """
    args_q = request.args
    db     = args_q.get("db")
    table  = args_q.get("table")
    if not db or not table:
        return jsonify({"error": "query params 'db' and 'table' are required"}), 400

    where = {k: v for k, v in args_q.items() if k not in ("db", "table")}
    query = f"SELECT * FROM `{db}`.`{table}`"
    args  = []
    cond, where_args = build_where(where)
    if cond:
        query += " WHERE " + cond
        args   = where_args

    conn = get_conn()
    cur  = conn.cursor()
    try:
        cur.execute(query, args)
        records = scan_rows(cur)
    finally:
        cur.close()
        conn.close()

    return jsonify({
        "count":     len(records),
        "records":   records,
        "served_by": "slave-python :8082",
    })


# ── SPECIAL FEATURE: Full-text search ─────────────────────────────────────
# This is the Python slave's unique contribution.
# It searches for a keyword across EVERY column of a table,
# something the Go slave does not do.
#
# GET /query/search?db=mydb&table=users&q=ali
# Returns every row where ANY column contains the search term (case-insensitive).

@app.route("/query/search", methods=["GET"])
def full_text_search():
    """
    Full-text search across all columns of a table.

    Example:
      GET /query/search?db=mydb&table=users&q=ali
      → returns every row where ANY column value contains 'ali'
        (case-insensitive substring match)
    """
    args_q = request.args
    db     = args_q.get("db")
    table  = args_q.get("table")
    term   = args_q.get("q", "").strip()

    if not db or not table:
        return jsonify({"error": "query params 'db' and 'table' are required"}), 400
    if not term:
        return jsonify({"error": "query param 'q' (search term) is required"}), 400

    # Fetch all rows, then filter in Python using case-insensitive substring match.
    # This lets us search across every column without knowing the schema upfront.
    conn = get_conn()
    cur  = conn.cursor()
    try:
        cur.execute(f"SELECT * FROM `{db}`.`{table}`")
        all_rows = scan_rows(cur)
    finally:
        cur.close()
        conn.close()

    term_lower = term.lower()
    matched = [
        row for row in all_rows
        if any(term_lower in str(v).lower() for v in row.values())
    ]

    return jsonify({
        "search_term": term,
        "count":       len(matched),
        "records":     matched,
        "served_by":   "slave-python :8082 (full-text search)",
    })


# ── Entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Python slave listening on :8082")
    app.run(host="0.0.0.0", port=8082, debug=False)