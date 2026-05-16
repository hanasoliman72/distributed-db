"""
Distributed Database GUI — Streamlit Version (Connected)
Run with:  streamlit run gui_streamlit.py

Connects to the Go gateway at http://127.0.0.1:8080
Theme: Dusty rose & deep plum — elegant, refined, feminine-luxe
"""

import streamlit as st
import json
import requests

# ─────────────────────────────────────────────────────────────────────────────
#  PAGE CONFIG
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Dist·DB Studio",
    page_icon="✦",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────────────────
#  GATEWAY CONFIG
# ─────────────────────────────────────────────────────────────────────────────
GATEWAY_URL = "http://127.0.0.1:8080"
REQUEST_TIMEOUT = 10  # seconds

# Supported MySQL column data types shown in the UI
COLUMN_TYPES = [
    "TEXT",
    "INT",
    "BIGINT",
    "FLOAT",
    "DOUBLE",
    "DECIMAL(10,2)",
    "BOOLEAN",
    "DATE",
    "DATETIME",
    "VARCHAR(255)",
    "VARCHAR(100)",
    "VARCHAR(50)",
    "JSON",
]

# ─────────────────────────────────────────────────────────────────────────────
#  CUSTOM CSS
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Cormorant+Garamond:ital,wght@0,400;0,500;0,600;1,400&family=DM+Mono:wght@400;500&display=swap');

:root {
    --parchment:  #ded8da;
    --plum:       #3d2b34;
    --rose:       #86476c;
    --stone:      #6e6d6b;
    --taupe:      #595851;
    --rose-light: #a6607f;
    --rose-deep:  #6a3456;
    --surface:    #2e1f27;
    --card:       #362430;
    --card-light: #3f2b38;
    --input-bg:   #281820;
    --border:     rgba(134,71,108,0.25);
    --border-hover: rgba(134,71,108,0.55);
    --white:      #ffffff;
}

html, body, [data-testid="stAppViewContainer"], .main {
    background: var(--surface) !important;
    color: var(--parchment) !important;
    font-family: 'DM Mono', monospace;
}

[data-testid="stSidebar"] {
    background: var(--plum) !important;
    border-right: 1px solid var(--border) !important;
}
[data-testid="stSidebar"] * { color: var(--parchment) !important; }

#MainMenu, footer, header { visibility: hidden; }
.block-container { padding-top: 1.2rem !important; padding-bottom: 2rem !important; }

h1, h2, h3, h4 {
    font-family: 'Cormorant Garamond', Georgia, serif !important;
    color: var(--parchment) !important;
    letter-spacing: 0.02em !important;
}

input[type="text"], textarea,
.stTextInput input, .stTextArea textarea {
    background: var(--input-bg) !important;
    color: var(--parchment) !important;
    border: 1px solid var(--border) !important;
    border-radius: 8px !important;
    font-family: 'DM Mono', monospace !important;
    font-size: 13px !important;
    padding: 10px 14px !important;
    transition: border-color 0.2s ease, box-shadow 0.2s ease !important;
}
input[type="text"]:focus, .stTextInput input:focus,
.stTextArea textarea:focus {
    border-color: var(--rose) !important;
    box-shadow: 0 0 0 3px rgba(134,71,108,0.18) !important;
    outline: none !important;
}

[data-testid="stSelectbox"] > div > div {
    background: var(--input-bg) !important;
    color: var(--parchment) !important;
    border-color: var(--border) !important;
    border-radius: 8px !important;
    font-family: 'DM Mono', monospace !important;
    font-size: 13px !important;
}

[data-testid="stNumberInput"] input {
    background: var(--input-bg) !important;
    color: var(--parchment) !important;
    border-color: var(--border) !important;
    border-radius: 8px !important;
    font-family: 'DM Mono', monospace !important;
}

.stButton > button {
    background: linear-gradient(135deg, var(--rose) 0%, var(--rose-deep) 100%) !important;
    color: var(--parchment) !important;
    border: none !important;
    border-radius: 10px !important;
    font-family: 'DM Mono', monospace !important;
    font-weight: 500 !important;
    font-size: 13px !important;
    padding: 12px 28px !important;
    letter-spacing: 0.06em !important;
    text-transform: uppercase !important;
    transition: all 0.2s ease !important;
    box-shadow: 0 4px 14px rgba(134,71,108,0.3) !important;
}
.stButton > button:hover {
    background: linear-gradient(135deg, var(--rose-light) 0%, var(--rose) 100%) !important;
    transform: translateY(-2px) !important;
    box-shadow: 0 8px 24px rgba(134,71,108,0.45) !important;
}
.stButton > button:active { transform: translateY(0) !important; }

.stTabs [data-baseweb="tab-list"] {
    background: var(--card) !important;
    border-radius: 14px !important;
    padding: 6px !important;
    gap: 4px !important;
    border: 1px solid var(--border) !important;
}
.stTabs [data-baseweb="tab"] {
    background: transparent !important;
    color: var(--stone) !important;
    font-family: 'Cormorant Garamond', Georgia, serif !important;
    font-size: 17px !important;
    font-weight: 500 !important;
    border-radius: 10px !important;
    padding: 10px 24px !important;
    transition: all 0.2s ease !important;
    letter-spacing: 0.03em !important;
}
.stTabs [data-baseweb="tab"]:hover {
    color: var(--parchment) !important;
    background: rgba(134,71,108,0.12) !important;
}
.stTabs [aria-selected="true"] {
    background: var(--rose) !important;
    color: var(--parchment) !important;
    font-style: italic !important;
}
.stTabs [data-baseweb="tab-panel"] {
    background: transparent !important;
    padding: 24px 0 0 0 !important;
}

[data-testid="stCheckbox"] { color: var(--parchment) !important; }
[data-testid="stCheckbox"] label span {
    color: var(--parchment) !important;
    font-size: 13px !important;
}

label, .stTextInput label, .stTextArea label,
.stSelectbox label, .stNumberInput label {
    font-family: 'DM Mono', monospace !important;
    font-size: 11px !important;
    font-weight: 500 !important;
    color: var(--stone) !important;
    letter-spacing: 0.1em !important;
    text-transform: uppercase !important;
}

hr { border-color: var(--border) !important; }

.result-box {
    background: var(--input-bg);
    border: 1px solid var(--border);
    border-left: 3px solid #7bbfb5;
    border-radius: 8px;
    padding: 14px 18px;
    font-family: 'DM Mono', monospace;
    font-size: 12px;
    color: #9dd8d2;
    margin-top: 10px;
    white-space: pre-wrap;
    line-height: 1.7;
    word-break: break-all;
}
.result-box.error {
    border-left-color: #c47a7a;
    color: #e8aaaa;
}
.result-box.warn {
    border-left-color: #c4a97a;
    color: #e8d0aa;
}

.section-card {
    background: var(--card);
    border-radius: 16px;
    padding: 28px 28px 24px;
    margin-bottom: 16px;
    border: 1px solid var(--border);
    position: relative;
    overflow: hidden;
}
.section-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 3px;
    border-radius: 16px 16px 0 0;
}
.card-rose::before   { background: var(--rose); }
.card-stone::before  { background: var(--stone); }
.card-taupe::before  { background: var(--taupe); }
.card-plum::before   { background: #9e607a; }

.section-card h3 {
    font-family: 'Cormorant Garamond', Georgia, serif !important;
    font-size: 22px !important;
    font-weight: 600 !important;
    font-style: italic !important;
    margin-bottom: 18px !important;
    color: var(--parchment) !important;
    letter-spacing: 0.02em !important;
}

.col-chip {
    display: inline-block;
    background: rgba(134,71,108,0.18);
    border: 1px solid rgba(134,71,108,0.4);
    color: var(--parchment);
    border-radius: 20px;
    padding: 4px 14px;
    font-size: 12px;
    margin: 3px 3px;
    font-family: 'DM Mono', monospace;
}

.badge {
    display: inline-block;
    font-size: 10px;
    font-family: 'DM Mono', monospace;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    padding: 3px 10px;
    border-radius: 20px;
    font-weight: 500;
}
.badge-rose {
    background: rgba(134,71,108,0.2);
    color: #c88eae;
    border: 1px solid rgba(134,71,108,0.4);
}
.badge-green {
    background: rgba(91,168,120,0.2);
    color: #8fd4a8;
    border: 1px solid rgba(91,168,120,0.4);
}
.badge-red {
    background: rgba(196,122,122,0.2);
    color: #e8aaaa;
    border: 1px solid rgba(196,122,122,0.4);
}

[data-testid="stMetric"] {
    background: var(--card-light);
    border-radius: 12px;
    padding: 16px 20px;
    border: 1px solid var(--border);
}
[data-testid="stMetricLabel"] { color: var(--stone) !important; font-size: 11px !important; }
[data-testid="stMetricValue"] { color: var(--parchment) !important; font-family: 'Cormorant Garamond', serif !important; font-size: 20px !important; }
[data-testid="stMetricDelta"] { color: var(--rose-light) !important; font-size: 12px !important; }

.hint-text {
    font-size: 11px;
    color: var(--stone);
    font-family: 'DM Mono', monospace;
    letter-spacing: 0.04em;
    margin-top: 6px;
}

.records-table {
    width: 100%;
    border-collapse: collapse;
    font-family: 'DM Mono', monospace;
    font-size: 12px;
    margin-top: 10px;
}
.records-table th {
    background: rgba(134,71,108,0.25);
    color: var(--parchment);
    padding: 8px 12px;
    text-align: left;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    font-size: 10px;
    border-bottom: 1px solid rgba(134,71,108,0.3);
}
.records-table td {
    padding: 8px 12px;
    color: #b8b0b4;
    border-bottom: 1px solid rgba(255,255,255,0.04);
}
.records-table tr:hover td {
    background: rgba(134,71,108,0.08);
}
.records-count {
    font-size: 11px;
    color: var(--stone);
    margin-top: 6px;
    font-family: 'DM Mono', monospace;
}
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
#  API CLIENT
# ─────────────────────────────────────────────────────────────────────────────
def api_post(path: str, payload: dict) -> tuple[bool, dict]:
    """POST to gateway. Returns (success, response_body)."""
    try:
        r = requests.post(f"{GATEWAY_URL}{path}", json=payload, timeout=REQUEST_TIMEOUT)
        body = r.json() if r.content else {}
        return r.status_code < 400, body
    except requests.exceptions.ConnectionError:
        return False, {"error": f"Cannot connect to gateway at {GATEWAY_URL}. Is it running?"}
    except Exception as e:
        return False, {"error": str(e)}


def api_delete(path: str, payload: dict) -> tuple[bool, dict]:
    """DELETE to gateway."""
    try:
        r = requests.delete(f"{GATEWAY_URL}{path}", json=payload, timeout=REQUEST_TIMEOUT)
        body = r.json() if r.content else {}
        return r.status_code < 400, body
    except requests.exceptions.ConnectionError:
        return False, {"error": f"Cannot connect to gateway at {GATEWAY_URL}. Is it running?"}
    except Exception as e:
        return False, {"error": str(e)}


def api_get(path: str, params: dict = None) -> tuple[bool, dict]:
    """GET from gateway."""
    try:
        r = requests.get(f"{GATEWAY_URL}{path}", params=params, timeout=REQUEST_TIMEOUT)
        body = r.json() if r.content else {}
        return r.status_code < 400, body
    except requests.exceptions.ConnectionError:
        return False, {"error": f"Cannot connect to gateway at {GATEWAY_URL}. Is it running?"}
    except Exception as e:
        return False, {"error": str(e)}


def api_put(path: str, payload: dict) -> tuple[bool, dict]:
    """PUT to gateway."""
    try:
        r = requests.put(f"{GATEWAY_URL}{path}", json=payload, timeout=REQUEST_TIMEOUT)
        body = r.json() if r.content else {}
        return r.status_code < 400, body
    except requests.exceptions.ConnectionError:
        return False, {"error": f"Cannot connect to gateway at {GATEWAY_URL}. Is it running?"}
    except Exception as e:
        return False, {"error": str(e)}


def check_gateway() -> tuple[bool, str]:
    """Ping the gateway /health endpoint."""
    try:
        r = requests.get(f"{GATEWAY_URL}/health", timeout=3)
        if r.status_code == 200:
            data = r.json()
            role = data.get("role", "unknown")
            promoted = data.get("promoted_by", "")
            label = f"Online · {role}"
            if promoted:
                label += f" (promoted from {promoted})"
            return True, label
        return False, f"HTTP {r.status_code}"
    except Exception as e:
        return False, str(e)


def get_gateway_status() -> dict:
    """Fetch /gateway/status for slave info."""
    try:
        r = requests.get(f"{GATEWAY_URL}/gateway/status", timeout=3)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return {}


# ─────────────────────────────────────────────────────────────────────────────
#  LOCAL METADATA CACHE
#  We keep a local cache of known DB→tables→columns so the UI can
#  show dropdowns without a dedicated /db/list endpoint.
#  The cache is pre-seeded from the gateway's metadata snapshot.
# ─────────────────────────────────────────────────────────────────────────────
def get_metadata_snapshot() -> dict:
    """Read the gateway's metadata.json snapshot for table info."""
    try:
        r = requests.get(f"{GATEWAY_URL}/gateway/status", timeout=3)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return {}


def init_local_cache():
    """Build local DB/table/column cache from gateway metadata snapshot."""
    if "local_meta" not in st.session_state:
        st.session_state.local_meta = {}  # {db: {table: [col, ...]}}

    status = get_metadata_snapshot()
    tables_meta = status.get("tables", [])
    if not tables_meta:
        return

    for tmeta in tables_meta:
        db = tmeta.get("DB", "")
        tbl = tmeta.get("Table", "")
        attrs = tmeta.get("Attributes", [])
        if db and tbl:
            if db not in st.session_state.local_meta:
                st.session_state.local_meta[db] = {}
            # Always include 'id' (auto) plus the stored attributes
            cols = ["id"] + [a for a in attrs if a.lower() != "id"]
            st.session_state.local_meta[db][tbl] = cols


def cache_add_db(db: str):
    if "local_meta" not in st.session_state:
        st.session_state.local_meta = {}
    if db not in st.session_state.local_meta:
        st.session_state.local_meta[db] = {}


def cache_remove_db(db: str):
    if "local_meta" in st.session_state:
        st.session_state.local_meta.pop(db, None)


def cache_add_table(db: str, table: str, cols: list):
    if "local_meta" not in st.session_state:
        st.session_state.local_meta = {}
    if db not in st.session_state.local_meta:
        st.session_state.local_meta[db] = {}
    st.session_state.local_meta[db][table] = ["id"] + [c for c in cols if c.lower() != "id"]


def cache_remove_table(db: str, table: str):
    if "local_meta" in st.session_state:
        st.session_state.local_meta.get(db, {}).pop(table, None)


def get_databases() -> list:
    if "local_meta" not in st.session_state:
        return []
    return list(st.session_state.local_meta.keys())


def get_tables(db: str) -> list:
    if "local_meta" not in st.session_state:
        return []
    return list(st.session_state.local_meta.get(db, {}).keys())


def get_columns(db: str, table: str) -> list:
    if "local_meta" not in st.session_state:
        return []
    return st.session_state.local_meta.get(db, {}).get(table, [])


# ─────────────────────────────────────────────────────────────────────────────
#  SESSION STATE — result log
# ─────────────────────────────────────────────────────────────────────────────
if "logs" not in st.session_state:
    st.session_state.logs = []

# Initialise local metadata cache on first run
init_local_cache()


def log(msg: str, kind: str = "ok"):
    icon = {"ok": "✦", "error": "✕", "warn": "◈"}.get(kind, "·")
    st.session_state.logs.append((f"{icon}  {msg}", kind))


def show_log():
    if st.session_state.logs:
        last_msg, last_kind = st.session_state.logs[-1]
        css_class = {
            "ok": "result-box",
            "error": "result-box error",
            "warn": "result-box warn"
        }.get(last_kind, "result-box")
        st.markdown(f'<div class="{css_class}">{last_msg}</div>', unsafe_allow_html=True)


def format_response(body: dict) -> str:
    return json.dumps(body, indent=2, default=str)


# ─────────────────────────────────────────────────────────────────────────────
#  SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style='text-align:center; padding: 20px 0 12px'>
        <div style='font-size:36px; color:#86476c; letter-spacing:8px'>✦ ✦ ✦</div>
        <div style='font-family: Cormorant Garamond, Georgia, serif;
                    font-size:28px; font-weight:600; font-style:italic;
                    color:#ded8da; margin-top:8px; letter-spacing:0.04em'>
            Dist·DB
        </div>
        <div style='font-size:11px; color:#6e6d6b; letter-spacing:0.12em;
                    text-transform:uppercase; margin-top:4px'>Studio</div>
    </div>
    <hr>
    """, unsafe_allow_html=True)

    # Gateway status
    gw_alive, gw_label = check_gateway()
    gw_color = "#5ba878" if gw_alive else "#c47a7a"
    gw_badge = "ONLINE" if gw_alive else "OFFLINE"
    st.markdown(f"""
    <div style='display:flex; align-items:center; gap:10px;
                background:rgba(255,255,255,0.04); border-radius:8px;
                padding:10px 12px; margin-bottom:12px;
                border:1px solid rgba(222,216,218,0.08)'>
        <div style='width:8px; height:8px; border-radius:50%;
                    background:{gw_color}; box-shadow:0 0 6px {gw_color}; flex-shrink:0'></div>
        <div style='flex:1'>
            <div style='font-size:12px; color:#ded8da; font-family:DM Mono,monospace'>
                Gateway &nbsp;<span style='font-size:9px; background:rgba(91,168,120,0.2);
                color:#8fd4a8; border-radius:10px; padding:2px 8px'>{gw_badge}</span>
            </div>
            <div style='font-size:10px; color:#6e6d6b; font-family:DM Mono,monospace'>{gw_label}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Slave nodes from gateway status
    st.markdown("""
    <div style='font-size:10px; color:#6e6d6b; letter-spacing:0.1em;
                text-transform:uppercase; margin-bottom:10px'>Cluster Nodes</div>
    """, unsafe_allow_html=True)

    gw_status = get_gateway_status()
    slaves = gw_status.get("slaves", [])
    # Fallback static list if gateway unreachable
    if not slaves:
        slaves = [
            {"id": "slave-a", "url": "http://127.0.0.1:8081", "alive": False},
            {"id": "slave-b", "url": "http://127.0.0.1:8082", "alive": False},
            {"id": "slave-c", "url": "http://127.0.0.1:8083", "alive": False},
        ]

    node_colors = ["#86476c", "#7a9e8e", "#7a8e9e", "#9e8e7a"]
    for i, slave in enumerate(slaves):
        color = node_colors[i % len(node_colors)]
        alive = slave.get("alive", False)
        dot_color = color if alive else "#595851"
        port = slave.get("url", "").split(":")[-1] if slave.get("url") else "?"
        status_label = "Alive" if alive else "Offline"
        st.markdown(f"""
        <div style='display:flex; align-items:center; gap:10px;
                    background:rgba(255,255,255,0.04); border-radius:8px;
                    padding:9px 12px; margin-bottom:6px;
                    border:1px solid rgba(222,216,218,0.08)'>
            <div style='width:8px; height:8px; border-radius:50%;
                        background:{dot_color}; box-shadow:0 0 6px {dot_color}; flex-shrink:0'></div>
            <div style='flex:1'>
                <div style='font-size:12px; color:#ded8da; font-family:DM Mono,monospace'>
                    {slave.get("id", "?")}
                </div>
                <div style='font-size:10px; color:#6e6d6b; font-family:DM Mono,monospace'>
                    :{port} · {status_label}
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<hr>", unsafe_allow_html=True)

    # Refresh metadata cache button
    if st.button("↻  Refresh Metadata"):
        st.session_state.local_meta = {}
        init_local_cache()
        log("Metadata cache refreshed from gateway.", "ok")
        st.rerun()

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("""
    <div style='font-size:10px; color:#6e6d6b; letter-spacing:0.1em;
                text-transform:uppercase; margin-bottom:8px'>Activity Log</div>
    """, unsafe_allow_html=True)

    if st.session_state.logs:
        log_html = "".join([
            f'<div style="padding:3px 0; font-size:11px; '
            f'color:{"#9dd8d2" if k=="ok" else "#e8aaaa" if k=="error" else "#e8d0aa"}">'
            f'{m}</div>'
            for m, k in st.session_state.logs[-15:]
        ])
        st.markdown(
            f'<div style="background:#1e1218; border-radius:8px; padding:10px 12px;'
            f'max-height:240px; overflow-y:auto; border:1px solid rgba(134,71,108,0.2)">'
            f'{log_html}</div>',
            unsafe_allow_html=True
        )
    else:
        st.markdown('<div style="font-size:11px; color:#6e6d6b; font-style:italic">No activity yet.</div>',
                    unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("✕  Clear Log"):
        st.session_state.logs = []
        st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
#  HEADER
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<div style='margin-bottom:4px'>
    <h1 style='font-family: Cormorant Garamond, Georgia, serif;
               font-size:42px; font-weight:600; font-style:italic;
               color:#ded8da; margin-bottom:4px; letter-spacing:0.02em'>
        ✦ &nbsp; Distributed Database Studio
    </h1>
    <p style='color:#6e6d6b; font-size:12px; margin:0; letter-spacing:0.08em;
              text-transform:uppercase; font-family:DM Mono,monospace'>
        Master–Slave Architecture &nbsp;·&nbsp; MySQL Sharding &nbsp;·&nbsp;
        Python · Go · .NET
    </p>
</div>
<hr>
""", unsafe_allow_html=True)

# Status metrics (live from gateway)
c1, c2, c3, c4 = st.columns(4)
alive_count = sum(1 for s in slaves if s.get("alive", False))
c1.metric("Gateway", "● Online" if gw_alive else "✕ Offline", f"Port :8080")
c2.metric("Slaves Online", f"{alive_count}/{len(slaves)}", "Shards active")
dropped = gw_status.get("dropped_dbs", [])
c3.metric("Dropped DBs", str(len(dropped)), ", ".join(dropped) if dropped else "none")
c4.metric("Metadata Ver.", str(gw_status.get("version", "—")), "sync'd")

st.markdown("<br>", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN TABS
# ─────────────────────────────────────────────────────────────────────────────
tab_db, tab_tbl, tab_query, tab_search = st.tabs([
    "◈  Databases",
    "⬡  Tables",
    "✦  Data & Queries",
    "⊹  Search",
])


# ══════════════════════════════════════════════════════════════════════════════
#  TAB 1 — DATABASE
# ══════════════════════════════════════════════════════════════════════════════
with tab_db:
    col1, col2 = st.columns(2, gap="large")

    with col1:
        st.markdown('<div class="section-card card-rose">', unsafe_allow_html=True)
        st.markdown("### ✦ Create a New Database")
        st.markdown('<p class="hint-text">Name must use lowercase letters, numbers, and underscores only.</p>', unsafe_allow_html=True)
        db_name = st.text_input("Database name", key="create_db_name", placeholder="e.g.  library")

        if st.button("Create Database →", key="btn_create_db"):
            if not db_name.strip():
                log("Please enter a database name.", "error")
            else:
                ok, body = api_post("/db/create", {"db": db_name.strip()})
                if ok:
                    cache_add_db(db_name.strip())
                    log(f"Database '{db_name.strip()}' created.\n{format_response(body)}", "ok")
                else:
                    log(f"Failed to create database '{db_name.strip()}'.\n{format_response(body)}", "error")

        show_log()
        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="section-card card-plum">', unsafe_allow_html=True)
        st.markdown("### ✕ Remove a Database")
        st.markdown('<p class="hint-text">Drops the primary database across all shards. Replica data is retained for fallback.</p>', unsafe_allow_html=True)
        dbs = get_databases()
        if dbs:
            db_drop = st.selectbox("Choose database to remove", dbs, key="drop_db_sel")
            confirm_drop = st.checkbox("Yes, I understand this cannot be undone", key="confirm_drop_db")
            if st.button("Remove Database →", key="btn_drop_db"):
                if not confirm_drop:
                    log("Please confirm before removing the database.", "warn")
                else:
                    ok, body = api_delete("/db/drop", {"db": db_drop})
                    if ok:
                        cache_remove_db(db_drop)
                        log(f"Database '{db_drop}' dropped.\n{format_response(body)}", "ok")
                        st.rerun()
                    else:
                        log(f"Failed to drop '{db_drop}'.\n{format_response(body)}", "error")
        else:
            st.markdown('<p class="hint-text" style="font-style:italic">No databases in local cache. Try refreshing metadata or create one first.</p>', unsafe_allow_html=True)
        show_log()
        st.markdown('</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
#  TAB 2 — TABLE
# ══════════════════════════════════════════════════════════════════════════════
with tab_tbl:
    col1, col2 = st.columns(2, gap="large")

    with col1:
        st.markdown('<div class="section-card card-rose">', unsafe_allow_html=True)
        st.markdown("### ⬡ Create a New Table")
        st.markdown('<p class="hint-text">The <strong>id</strong> column is added automatically as an auto-increment primary key — do not include it below.</p>', unsafe_allow_html=True)

        dbs = get_databases()
        if not dbs:
            st.markdown('<p class="hint-text" style="font-style:italic">Create a database first.</p>', unsafe_allow_html=True)
        else:
            tbl_db   = st.selectbox("Which database?", dbs, key="create_tbl_db")
            tbl_name = st.text_input("Table name", key="create_tbl_name", placeholder="e.g.  students")

            num_cols = st.number_input(
                "How many columns? (excluding id)",
                min_value=1, max_value=20, value=2, step=1,
                key="create_tbl_num_cols"
            )

            st.markdown('<p class="hint-text" style="margin-top:10px;">Define each column name and its data type:</p>', unsafe_allow_html=True)

            col_defs = []  # list of (name, type) tuples
            for i in range(int(num_cols)):
                c_name_col, c_type_col = st.columns([2, 1])
                with c_name_col:
                    cname = st.text_input(
                        f"Column {i+1} name",
                        key=f"col_name_{i}",
                        placeholder=f"e.g. column_{i+1}"
                    )
                with c_type_col:
                    ctype = st.selectbox(
                        f"Type",
                        COLUMN_TYPES,
                        key=f"col_type_{i}",
                        index=0
                    )
                col_defs.append((cname.strip(), ctype))

            # Show id auto note
            st.markdown("""
            <div style="background:rgba(134,71,108,0.12); border:1px solid rgba(134,71,108,0.3);
                        border-radius:8px; padding:10px 14px; margin-top:10px;">
                <span style="font-size:11px; font-family:DM Mono,monospace; color:#a6607f;">
                    ✦ &nbsp;<strong>id</strong> &nbsp;INT AUTO_INCREMENT PRIMARY KEY
                    &nbsp;— added automatically
                </span>
            </div>
            """, unsafe_allow_html=True)

            if st.button("Create Table →", key="btn_create_tbl"):
                bad = [n for n, _ in col_defs if n == ""]
                if not tbl_name.strip():
                    log("Please enter a table name.", "error")
                elif bad:
                    log("Please fill in all column names.", "error")
                else:
                    # Send to gateway: attributes are "name:TYPE" strings
                    # The backend uses TEXT for all; we store type info locally
                    # Note: gateway CreateTable only uses attribute *names* for DDL
                    # and appends TEXT. The type info is informational in the UI.
                    attrs = [n for n, _ in col_defs]
                    ok, body = api_post("/table/create", {
                        "db": tbl_db,
                        "table": tbl_name.strip(),
                        "attributes": attrs
                    })
                    if ok:
                        # Store column names (with types annotated) locally
                        cache_add_table(tbl_db, tbl_name.strip(), attrs)
                        col_labels = " ".join([
                            f'<span class="col-chip">{n} <span style="opacity:0.6">·{t}</span></span>'
                            for n, t in col_defs
                        ])
                        log(f"Table '{tbl_name.strip()}' created in '{tbl_db}'.\n{format_response(body)}", "ok")
                        st.markdown(f'<div style="margin-top:8px">{col_labels}</div>', unsafe_allow_html=True)
                    else:
                        log(f"Failed to create table.\n{format_response(body)}", "error")

        show_log()
        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="section-card card-plum">', unsafe_allow_html=True)
        st.markdown("### ✕ Remove a Table")
        st.markdown('<p class="hint-text">Permanently deletes the table from all shards and replicas.</p>', unsafe_allow_html=True)

        dbs = get_databases()
        if not dbs:
            st.markdown('<p class="hint-text" style="font-style:italic">No databases available.</p>', unsafe_allow_html=True)
        else:
            drop_db = st.selectbox("Which database?", dbs, key="drop_tbl_db_sel")
            tables  = get_tables(drop_db)
            if tables:
                drop_tbl = st.selectbox("Which table?", tables, key="drop_tbl_sel")
                cols_preview = get_columns(drop_db, drop_tbl)
                if cols_preview:
                    chips = "".join([f'<span class="col-chip">{c}</span>' for c in cols_preview])
                    st.markdown(f'<p class="hint-text">Columns: {chips}</p>', unsafe_allow_html=True)
                confirm_tbl = st.checkbox("Yes, delete this table and all its data", key="confirm_drop_tbl")
                if st.button("Remove Table →", key="btn_drop_tbl"):
                    if not confirm_tbl:
                        log("Please confirm before removing the table.", "warn")
                    else:
                        ok, body = api_delete("/table/drop", {"db": drop_db, "table": drop_tbl})
                        if ok:
                            cache_remove_table(drop_db, drop_tbl)
                            log(f"Table '{drop_tbl}' removed from '{drop_db}'.\n{format_response(body)}", "ok")
                            st.rerun()
                        else:
                            log(f"Failed to drop table.\n{format_response(body)}", "error")
            else:
                st.markdown('<p class="hint-text" style="font-style:italic">No tables in this database yet.</p>', unsafe_allow_html=True)

        show_log()
        st.markdown('</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
#  TAB 3 — QUERY
# ══════════════════════════════════════════════════════════════════════════════
with tab_query:
    q1, q2, q3, q4 = st.tabs([
        "✦  Add Record",
        "◎  View Records",
        "⬡  Edit Record",
        "✕  Delete Record",
    ])

    # ── shared selector ──────────────────────────────────────────────────────
    def db_table_selectors(prefix: str):
        dbs = get_databases()
        if not dbs:
            st.markdown('<p class="hint-text" style="font-style:italic">No databases in cache — create one or refresh metadata.</p>', unsafe_allow_html=True)
            return None, None, []
        chosen_db  = st.selectbox("Which database?", dbs, key=f"{prefix}_db")
        tables     = get_tables(chosen_db)
        if not tables:
            st.markdown('<p class="hint-text" style="font-style:italic">No tables in this database.</p>', unsafe_allow_html=True)
            return chosen_db, None, []
        chosen_tbl = st.selectbox("Which table?", tables, key=f"{prefix}_tbl")
        cols       = [c for c in get_columns(chosen_db, chosen_tbl) if c.lower() != "id"]
        return chosen_db, chosen_tbl, cols

    # ── INSERT ──────────────────────────────────────────────────────────────
    with q1:
        st.markdown('<div class="section-card card-rose">', unsafe_allow_html=True)
        st.markdown("### ✦ Add a New Record")
        st.markdown('<p class="hint-text">The <strong>id</strong> is assigned automatically — fill in the other fields below.</p>', unsafe_allow_html=True)

        ins_db, ins_tbl, ins_cols = db_table_selectors("ins")

        if ins_cols:
            st.markdown("<br>", unsafe_allow_html=True)
            ins_values = {}
            ncols_ui = 2 if len(ins_cols) >= 2 else 1
            col_pairs = st.columns(ncols_ui)
            for i, col in enumerate(ins_cols):
                with col_pairs[i % ncols_ui]:
                    val = st.text_input(col.capitalize(), key=f"ins_val_{col}", placeholder=f"Enter {col}…")
                    ins_values[col] = val

            # Auto-id note
            st.markdown("""
            <div style="background:rgba(134,71,108,0.08); border-radius:6px;
                        padding:8px 12px; margin:8px 0; font-size:11px;
                        font-family:DM Mono,monospace; color:#a6607f;">
                ✦ id will be auto-generated by the database
            </div>
            """, unsafe_allow_html=True)

            if st.button("Add Record →", key="btn_insert"):
                if any(v.strip() == "" for v in ins_values.values()):
                    log("Please fill in all fields before saving.", "error")
                else:
                    record = {k: v.strip() for k, v in ins_values.items()}
                    ok, body = api_post("/query/insert", {
                        "db": ins_db,
                        "table": ins_tbl,
                        "record": record
                    })
                    if ok:
                        gen_id = body.get("generated_id", "?")
                        shard  = body.get("shard", "?")
                        log(f"Record added to '{ins_tbl}' in '{ins_db}'.\n"
                            f"  generated_id={gen_id}  shard={shard}\n"
                            f"  data={json.dumps(record)}", "ok")
                    else:
                        log(f"Insert failed.\n{format_response(body)}", "error")

        show_log()
        st.markdown('</div>', unsafe_allow_html=True)

    # ── SELECT ──────────────────────────────────────────────────────────────
    with q2:
        st.markdown('<div class="section-card card-stone">', unsafe_allow_html=True)
        st.markdown("### ◎ View Records")
        st.markdown('<p class="hint-text">Choose a table and optionally filter by a specific field.</p>', unsafe_allow_html=True)

        sel_db, sel_tbl, sel_cols = db_table_selectors("sel")

        if sel_tbl:
            all_cols = get_columns(sel_db, sel_tbl)
            filter_options = ["Show all records"] + all_cols
            sel_filter_col = st.selectbox("Filter by field (optional)", filter_options, key="sel_filter_col")

            sel_filter_val = ""
            if sel_filter_col != "Show all records":
                sel_filter_val = st.text_input(
                    f"Value to match in '{sel_filter_col}'",
                    key="sel_filter_val",
                    placeholder=f"e.g.  42"
                )

            if st.button("View Records →", key="btn_select"):
                params = {"db": sel_db, "table": sel_tbl}
                if sel_filter_col != "Show all records" and sel_filter_val.strip():
                    params[sel_filter_col] = sel_filter_val.strip()

                ok, body = api_get("/query/select", params)
                if ok:
                    records = body.get("records", [])
                    count   = body.get("count", len(records))

                    if records:
                        # Build HTML table
                        headers = list(records[0].keys())
                        thead = "".join(f"<th>{h}</th>" for h in headers)
                        rows_html = ""
                        for rec in records:
                            cells = "".join(f"<td>{rec.get(h, '')}</td>" for h in headers)
                            rows_html += f"<tr>{cells}</tr>"
                        table_html = f"""
                        <table class="records-table">
                            <thead><tr>{thead}</tr></thead>
                            <tbody>{rows_html}</tbody>
                        </table>
                        <p class="records-count">↳ {count} record(s) returned</p>
                        """
                        log(f"Fetched {count} record(s) from '{sel_tbl}'.", "ok")
                        st.markdown(table_html, unsafe_allow_html=True)
                    else:
                        log(f"No records found in '{sel_tbl}'.", "warn")
                else:
                    log(f"Select failed.\n{format_response(body)}", "error")

        show_log()
        st.markdown('</div>', unsafe_allow_html=True)

    # ── UPDATE ──────────────────────────────────────────────────────────────
    with q3:
        st.markdown('<div class="section-card card-taupe">', unsafe_allow_html=True)
        st.markdown("### ⬡ Edit a Record")
        st.markdown('<p class="hint-text">Identify the record by a field value, then set the new value.</p>', unsafe_allow_html=True)

        upd_db, upd_tbl, upd_cols = db_table_selectors("upd")

        if upd_cols:
            all_upd_cols = get_columns(upd_db, upd_tbl)

            st.markdown('<p class="hint-text" style="margin-top:14px; font-size:12px; color:#9e8a7a">Step 1 — Identify the record</p>', unsafe_allow_html=True)
            upd_find_col = st.selectbox("Find records where…", all_upd_cols, key="upd_find_col")
            upd_find_val = st.text_input("…equals", key="upd_find_val", placeholder="Enter value to match…")

            st.markdown('<p class="hint-text" style="margin-top:14px; font-size:12px; color:#9e8a7a">Step 2 — Choose what to change</p>', unsafe_allow_html=True)
            upd_set_col  = st.selectbox("Update this field", upd_cols, key="upd_set_col")
            upd_set_val  = st.text_input("New value", key="upd_set_val", placeholder="Enter the new value…")

            if st.button("Save Changes →", key="btn_update"):
                if not upd_find_val.strip() or not upd_set_val.strip():
                    log("Please fill in both the search value and the new value.", "error")
                else:
                    ok, body = api_put("/query/update", {
                        "db": upd_db,
                        "table": upd_tbl,
                        "where": {upd_find_col: upd_find_val.strip()},
                        "set":   {upd_set_col: upd_set_val.strip()}
                    })
                    if ok:
                        n = body.get("records_updated", "?")
                        log(f"Updated '{upd_set_col}' → '{upd_set_val.strip()}' "
                            f"where {upd_find_col}='{upd_find_val.strip()}'. "
                            f"{n} record(s) affected.", "ok")
                    else:
                        log(f"Update failed.\n{format_response(body)}", "error")

        show_log()
        st.markdown('</div>', unsafe_allow_html=True)

    # ── DELETE ──────────────────────────────────────────────────────────────
    with q4:
        st.markdown('<div class="section-card card-plum">', unsafe_allow_html=True)
        st.markdown("### ✕ Delete Records")
        st.markdown('<p class="hint-text">Specify which records to delete — this cannot be undone.</p>', unsafe_allow_html=True)

        del_db, del_tbl, del_cols = db_table_selectors("del")

        if del_tbl:
            all_del_cols = get_columns(del_db, del_tbl)
            del_find_col = st.selectbox("Delete records where…", all_del_cols, key="del_find_col")
            del_find_val = st.text_input("…equals", key="del_find_val", placeholder="Enter value to match…")
            confirm_del  = st.checkbox("Yes, permanently delete these records", key="confirm_del")

            if st.button("Delete Records →", key="btn_delete"):
                if not del_find_val.strip():
                    log("Please specify which records to delete.", "error")
                elif not confirm_del:
                    log("Please confirm before deleting.", "warn")
                else:
                    ok, body = api_delete("/query/delete", {
                        "db": del_db,
                        "table": del_tbl,
                        "where": {del_find_col: del_find_val.strip()}
                    })
                    if ok:
                        n = body.get("records_deleted", "?")
                        log(f"Deleted {n} record(s) from '{del_tbl}' "
                            f"where {del_find_col}='{del_find_val.strip()}'.", "ok")
                    else:
                        log(f"Delete failed.\n{format_response(body)}", "error")

        show_log()
        st.markdown('</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
#  TAB 4 — SEARCH
# ══════════════════════════════════════════════════════════════════════════════
with tab_search:
    st.markdown('<div class="section-card card-rose">', unsafe_allow_html=True)
    st.markdown("### ⊹ Full-Text Search")
    st.markdown('<p class="hint-text">Search across all fields in a table. Results are merged from all shards.</p>', unsafe_allow_html=True)

    dbs = get_databases()
    if not dbs:
        st.markdown('<p class="hint-text" style="font-style:italic">No databases in cache.</p>', unsafe_allow_html=True)
    else:
        srch_db  = st.selectbox("Database", dbs, key="srch_db")
        srch_tbls = get_tables(srch_db)
        if srch_tbls:
            srch_tbl  = st.selectbox("Table", srch_tbls, key="srch_tbl")
            srch_term = st.text_input("Search term", key="srch_term", placeholder="e.g.  Alice")

            if st.button("Search →", key="btn_search"):
                if not srch_term.strip():
                    log("Please enter a search term.", "error")
                else:
                    ok, body = api_get("/query/search", {
                        "db": srch_db,
                        "table": srch_tbl,
                        "q": srch_term.strip()
                    })
                    if ok:
                        records = body.get("records", [])
                        count   = body.get("count", len(records))

                        if records:
                            headers = list(records[0].keys())
                            thead = "".join(f"<th>{h}</th>" for h in headers)
                            rows_html = ""
                            for rec in records:
                                cells = "".join(f"<td>{rec.get(h, '')}</td>" for h in headers)
                                rows_html += f"<tr>{cells}</tr>"
                            table_html = f"""
                            <table class="records-table">
                                <thead><tr>{thead}</tr></thead>
                                <tbody>{rows_html}</tbody>
                            </table>
                            <p class="records-count">↳ {count} match(es) for "{srch_term.strip()}"</p>
                            """
                            log(f"Search '{srch_term.strip()}' → {count} match(es).", "ok")
                            st.markdown(table_html, unsafe_allow_html=True)
                        else:
                            log(f"No matches for '{srch_term.strip()}'.", "warn")
                    else:
                        log(f"Search failed.\n{format_response(body)}", "error")
        else:
            st.markdown('<p class="hint-text" style="font-style:italic">No tables in this database.</p>', unsafe_allow_html=True)

    show_log()
    st.markdown('</div>', unsafe_allow_html=True)