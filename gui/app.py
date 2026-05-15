"""
Distributed Database GUI — Streamlit Version (Redesigned)
Run with:  streamlit run gui_streamlit.py

Theme: Dusty rose & deep plum — elegant, refined, feminine-luxe
Palette:
  #ded8da  — parchment (lightest surface / text on dark)
  #3d2b34  — deep plum (darkest bg, sidebar)
  #86476c  — antique rose (primary accent)
  #6e6d6b  — warm stone (muted text)
  #595851  — charcoal taupe (secondary surface)
"""

import streamlit as st
import json

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

/* ── Headings ── */
h1, h2, h3, h4 {
    font-family: 'Cormorant Garamond', Georgia, serif !important;
    color: var(--parchment) !important;
    letter-spacing: 0.02em !important;
}

/* ── Inputs & Textareas ── */
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

/* ── Selectbox ── */
[data-testid="stSelectbox"] > div > div {
    background: var(--input-bg) !important;
    color: var(--parchment) !important;
    border-color: var(--border) !important;
    border-radius: 8px !important;
    font-family: 'DM Mono', monospace !important;
    font-size: 13px !important;
}

/* ── Number input ── */
[data-testid="stNumberInput"] input {
    background: var(--input-bg) !important;
    color: var(--parchment) !important;
    border-color: var(--border) !important;
    border-radius: 8px !important;
    font-family: 'DM Mono', monospace !important;
}

/* ── Buttons ── */
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
.stButton > button:active {
    transform: translateY(0) !important;
}

/* ── Tabs ── */
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

/* ── Checkbox ── */
[data-testid="stCheckbox"] {
    color: var(--parchment) !important;
}
[data-testid="stCheckbox"] label span {
    color: var(--parchment) !important;
    font-size: 13px !important;
}

/* ── Labels ── */
label, .stTextInput label, .stTextArea label,
.stSelectbox label, .stNumberInput label {
    font-family: 'DM Mono', monospace !important;
    font-size: 11px !important;
    font-weight: 500 !important;
    color: var(--stone) !important;
    letter-spacing: 0.1em !important;
    text-transform: uppercase !important;
}

/* ── Divider ── */
hr { border-color: var(--border) !important; }

/* ── Result boxes ── */
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
}
.result-box.error {
    border-left-color: #c47a7a;
    color: #e8aaaa;
}
.result-box.warn {
    border-left-color: #c4a97a;
    color: #e8d0aa;
}

/* ── Section cards ── */
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

/* ── Column name chips (for table creation preview) ── */
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

/* ── Badge ── */
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

/* ── Metric ── */
[data-testid="stMetric"] {
    background: var(--card-light);
    border-radius: 12px;
    padding: 16px 20px;
    border: 1px solid var(--border);
}
[data-testid="stMetricLabel"] { color: var(--stone) !important; font-size: 11px !important; }
[data-testid="stMetricValue"] { color: var(--parchment) !important; font-family: 'Cormorant Garamond', serif !important; font-size: 20px !important; }
[data-testid="stMetricDelta"] { color: var(--rose-light) !important; font-size: 12px !important; }

/* ── Info text ── */
.hint-text {
    font-size: 11px;
    color: var(--stone);
    font-family: 'DM Mono', monospace;
    letter-spacing: 0.04em;
    margin-top: 6px;
}

/* ── Separator ornament ── */
.ornament {
    text-align: center;
    color: var(--rose);
    font-size: 18px;
    letter-spacing: 12px;
    margin: 8px 0;
    opacity: 0.6;
}
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
#  MOCK DATA STORE  (replace with real API calls)
# ─────────────────────────────────────────────────────────────────────────────
if "databases" not in st.session_state:
    st.session_state.databases = {
        "university": {
            "students":  ["id", "name", "age", "email"],
            "courses":   ["id", "title", "credits", "instructor"],
            "enrollments": ["student_id", "course_id", "grade"],
        },
        "hospital": {
            "patients":  ["id", "full_name", "dob", "ward"],
            "doctors":   ["id", "name", "specialty", "phone"],
        }
    }


def get_databases():
    return list(st.session_state.databases.keys())

def get_tables(db_name):
    if not db_name or db_name not in st.session_state.databases:
        return []
    return list(st.session_state.databases[db_name].keys())

def get_columns(db_name, tbl_name):
    if not db_name or not tbl_name:
        return []
    return st.session_state.databases.get(db_name, {}).get(tbl_name, [])


# ─────────────────────────────────────────────────────────────────────────────
#  SESSION STATE — result log
# ─────────────────────────────────────────────────────────────────────────────
if "logs" not in st.session_state:
    st.session_state.logs = []

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

def validate_json(text: str):
    try:
        return True, json.loads(text)
    except Exception:
        return False, {}


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

    st.markdown("""
    <div style='font-size:10px; color:#6e6d6b; letter-spacing:0.1em;
                text-transform:uppercase; margin-bottom:10px'>Cluster Nodes</div>
    """, unsafe_allow_html=True)

    nodes = [
        ("Master",    "#86476c", ":8080", "Write"),
        ("Slave · .NET", "#7a9e8e", ":8081", "Read"),
        ("Slave · Python", "#7a8e9e", ":8082", "Read"),
        ("Slave · Go",  "#9e8e7a", ":8083", "Read"),
    ]
    for name, color, port, role in nodes:
        st.markdown(f"""
        <div style='display:flex; align-items:center; gap:10px;
                    background:rgba(255,255,255,0.04); border-radius:8px;
                    padding:9px 12px; margin-bottom:6px;
                    border:1px solid rgba(222,216,218,0.08)'>
            <div style='width:8px; height:8px; border-radius:50%;
                        background:{color}; box-shadow:0 0 6px {color}; flex-shrink:0'></div>
            <div style='flex:1'>
                <div style='font-size:12px; color:#ded8da; font-family:DM Mono,monospace'>{name}</div>
                <div style='font-size:10px; color:#6e6d6b; font-family:DM Mono,monospace'>{port} · {role}</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<hr>", unsafe_allow_html=True)

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
        Master–Slave Architecture &nbsp;·&nbsp; MySQL Replication &nbsp;·&nbsp;
        Python · Go · .NET
    </p>
</div>
<hr>
""", unsafe_allow_html=True)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Master Node",    "● Online",  "Write · :8080")
c2.metric(".NET Replica",   "● Online",  "Read  · :8081")
c3.metric("Python Replica", "● Online",  "Read  · :8082")
c4.metric("Go Replica",     "● Online",  "Read  · :8083")

st.markdown("<br>", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
#  MAIN TABS
# ─────────────────────────────────────────────────────────────────────────────
tab_db, tab_tbl, tab_query = st.tabs([
    "◈  Databases",
    "⬡  Tables",
    "✦  Data & Queries",
])


# ══════════════════════════════════════════════════════════════════════════════
#  TAB 1 — DATABASE
# ══════════════════════════════════════════════════════════════════════════════
with tab_db:
    col1, col2 = st.columns(2, gap="large")

    with col1:
        st.markdown('<div class="section-card card-rose">', unsafe_allow_html=True)
        st.markdown("### ✦ Create a New Database")
        st.markdown('<p class="hint-text">Choose a name for your new database. Use lowercase letters and underscores.</p>', unsafe_allow_html=True)
        db_name = st.text_input("Database name", key="create_db_name",
                                placeholder="e.g.  library")
        if st.button("Create Database →", key="btn_create_db"):
            if not db_name.strip():
                log("Please enter a database name.", "error")
            elif db_name.strip() in st.session_state.databases:
                log(f"A database named '{db_name.strip()}' already exists.", "warn")
            else:
                st.session_state.databases[db_name.strip()] = {}
                log(f"Database '{db_name.strip()}' created successfully.", "ok")
        show_log()
        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="section-card card-plum">', unsafe_allow_html=True)
        st.markdown("### ✕ Remove a Database")
        st.markdown('<p class="hint-text">Select a database to permanently delete it along with all its tables and data.</p>', unsafe_allow_html=True)
        dbs = get_databases()
        if dbs:
            db_drop = st.selectbox("Choose database to remove", dbs, key="drop_db_sel")
            confirm_drop = st.checkbox("Yes, I understand this cannot be undone", key="confirm_drop_db")
            if st.button("Remove Database →", key="btn_drop_db"):
                if not confirm_drop:
                    log("Please confirm before removing the database.", "warn")
                else:
                    del st.session_state.databases[db_drop]
                    log(f"Database '{db_drop}' removed.", "ok")
                    st.rerun()
        else:
            st.markdown('<p class="hint-text" style="font-style:italic">No databases exist yet. Create one first.</p>', unsafe_allow_html=True)
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
        st.markdown('<p class="hint-text">Choose a database, name your table, then define each column.</p>', unsafe_allow_html=True)

        dbs = get_databases()
        if not dbs:
            st.markdown('<p class="hint-text" style="font-style:italic">Create a database first before adding tables.</p>', unsafe_allow_html=True)
        else:
            tbl_db   = st.selectbox("Which database?", dbs, key="create_tbl_db")
            tbl_name = st.text_input("Table name", key="create_tbl_name", placeholder="e.g.  students")

            num_cols = st.number_input(
                "How many columns does this table have?",
                min_value=1, max_value=20, value=3, step=1,
                key="create_tbl_num_cols"
            )

            col_names = []
            if num_cols:
                st.markdown('<p class="hint-text">Enter a name for each column below:</p>', unsafe_allow_html=True)
                ncols_ui = 2 if num_cols >= 2 else 1
                col_pairs = st.columns(ncols_ui)
                for i in range(int(num_cols)):
                    with col_pairs[i % ncols_ui]:
                        cname = st.text_input(
                            f"Column {i+1}",
                            key=f"col_name_{i}",
                            placeholder=f"column_{i+1}"
                        )
                        col_names.append(cname.strip())

            if st.button("Create Table →", key="btn_create_tbl"):
                if not tbl_name.strip():
                    log("Please enter a table name.", "error")
                elif any(c == "" for c in col_names):
                    log("Please fill in all column names.", "error")
                elif tbl_name.strip() in st.session_state.databases.get(tbl_db, {}):
                    log(f"Table '{tbl_name.strip()}' already exists in '{tbl_db}'.", "warn")
                else:
                    final_cols = col_names
                    st.session_state.databases[tbl_db][tbl_name.strip()] = final_cols
                    log(f"Table '{tbl_name.strip()}' created in '{tbl_db}' with columns: {', '.join(final_cols)}.", "ok")

                    chips = "".join([f'<span class="col-chip">{c}</span>' for c in final_cols])
                    st.markdown(f'<div style="margin-top:8px">{chips}</div>', unsafe_allow_html=True)

        show_log()
        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="section-card card-plum">', unsafe_allow_html=True)
        st.markdown("### ✕ Remove a Table")
        st.markdown('<p class="hint-text">Select a database and table to permanently delete.</p>', unsafe_allow_html=True)

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
                        del st.session_state.databases[drop_db][drop_tbl]
                        log(f"Table '{drop_tbl}' removed from '{drop_db}'.", "ok")
                        st.rerun()
            else:
                st.markdown('<p class="hint-text" style="font-style:italic">No tables in this database yet.</p>', unsafe_allow_html=True)

        show_log()
        st.markdown('</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
#  TAB 3 — QUERY  (no JSON, fully guided)
# ══════════════════════════════════════════════════════════════════════════════
with tab_query:
    q1, q2, q3, q4 = st.tabs([
        "✦  Add Record",
        "◎  View Records",
        "⬡  Edit Record",
        "✕  Delete Record",
    ])

    # ── shared helper ──────────────────────────────────────────────────────
    def db_table_selectors(prefix: str):
        dbs = get_databases()
        if not dbs:
            st.markdown('<p class="hint-text" style="font-style:italic">No databases yet — go to the Databases tab to create one.</p>', unsafe_allow_html=True)
            return None, None, []
        chosen_db  = st.selectbox("Which database?", dbs, key=f"{prefix}_db")
        tables     = get_tables(chosen_db)
        if not tables:
            st.markdown('<p class="hint-text" style="font-style:italic">No tables in this database — go to Tables to create one first.</p>', unsafe_allow_html=True)
            return chosen_db, None, []
        chosen_tbl = st.selectbox("Which table?", tables, key=f"{prefix}_tbl")
        cols       = get_columns(chosen_db, chosen_tbl)
        return chosen_db, chosen_tbl, cols


    # ── INSERT ─────────────────────────────────────────────────────────────
    with q1:
        st.markdown('<div class="section-card card-rose">', unsafe_allow_html=True)
        st.markdown("### ✦ Add a New Record")
        st.markdown('<p class="hint-text">Choose where to add the record, then fill in each field.</p>', unsafe_allow_html=True)

        ins_db, ins_tbl, ins_cols = db_table_selectors("ins")

        ins_values = {}
        if ins_cols:
            st.markdown("<br>", unsafe_allow_html=True)
            st.markdown('<p class="hint-text" style="margin-bottom:8px">Fill in the values for each field:</p>', unsafe_allow_html=True)
            ncols_ui = 2 if len(ins_cols) >= 2 else 1
            col_pairs = st.columns(ncols_ui)
            for i, col in enumerate(ins_cols):
                with col_pairs[i % ncols_ui]:
                    val = st.text_input(col.capitalize(), key=f"ins_val_{col}", placeholder=f"Enter {col}…")
                    ins_values[col] = val

            if st.button("Add Record →", key="btn_insert"):
                if any(v.strip() == "" for v in ins_values.values()):
                    log("Please fill in all fields before saving.", "error")
                else:
                    record = {k: v.strip() for k, v in ins_values.items()}
                    # TODO: requests.post("http://localhost:8080/query/insert", json={
                    #     "db": ins_db, "table": ins_tbl, "record": record})
                    log(f"Record added to '{ins_tbl}' in '{ins_db}'.\n   Data: {record}", "ok")

        show_log()
        st.markdown('</div>', unsafe_allow_html=True)


    # ── SELECT ─────────────────────────────────────────────────────────────
    with q2:
        st.markdown('<div class="section-card card-stone">', unsafe_allow_html=True)
        st.markdown("### ◎ View Records")
        st.markdown('<p class="hint-text">Choose a table and optionally filter by a specific field value.</p>', unsafe_allow_html=True)

        sel_db, sel_tbl, sel_cols = db_table_selectors("sel")

        if sel_cols:
            filter_options = ["Show all records"] + sel_cols
            sel_filter_col = st.selectbox("Filter by field (optional)", filter_options, key="sel_filter_col")

            sel_filter_val = ""
            if sel_filter_col != "Show all records":
                sel_filter_val = st.text_input(
                    f"Value to search for in '{sel_filter_col}'",
                    key="sel_filter_val",
                    placeholder=f"e.g.  Ali"
                )

            if st.button("View Records →", key="btn_select"):
                where = {}
                if sel_filter_col != "Show all records" and sel_filter_val.strip():
                    where = {sel_filter_col: sel_filter_val.strip()}
                # TODO: requests.get("http://localhost:{port}/query/select", json={
                #     "db": sel_db, "table": sel_tbl, "where": where})
                # Backend picks the best read node automatically
                filter_desc = f"where {sel_filter_col} = '{sel_filter_val.strip()}'" if where else "all records"
                log(f"Fetching {filter_desc} from '{sel_tbl}' in '{sel_db}'.\n   Sample result: [{{\"id\":1,\"name\":\"Ali\",\"age\":\"20\"}}]", "ok")

        show_log()
        st.markdown('</div>', unsafe_allow_html=True)


    # ── UPDATE ─────────────────────────────────────────────────────────────
    with q3:
        st.markdown('<div class="section-card card-taupe">', unsafe_allow_html=True)
        st.markdown("### ⬡ Edit a Record")
        st.markdown('<p class="hint-text">First identify the record you want to change, then set the new value.</p>', unsafe_allow_html=True)

        upd_db, upd_tbl, upd_cols = db_table_selectors("upd")

        if upd_cols:
            st.markdown('<p class="hint-text" style="margin-top:14px; font-size:12px; color:#9e8a7a">Step 1 — Identify the record</p>', unsafe_allow_html=True)
            upd_find_col = st.selectbox("Find records where this field…", upd_cols, key="upd_find_col")
            upd_find_val = st.text_input(f"…equals", key="upd_find_val", placeholder=f"Enter value to match…")

            st.markdown('<p class="hint-text" style="margin-top:14px; font-size:12px; color:#9e8a7a">Step 2 — Choose what to change</p>', unsafe_allow_html=True)
            upd_set_col  = st.selectbox("Update this field", upd_cols, key="upd_set_col")
            upd_set_val  = st.text_input("New value", key="upd_set_val", placeholder="Enter the new value…")

            if st.button("Save Changes →", key="btn_update"):
                if not upd_find_val.strip() or not upd_set_val.strip():
                    log("Please fill in both the search value and the new value.", "error")
                else:
                    where = {upd_find_col: upd_find_val.strip()}
                    updates = {upd_set_col: upd_set_val.strip()}
                    # TODO: requests.put("http://localhost:8080/query/update", json={
                    #     "db": upd_db, "table": upd_tbl, "where": where, "set": updates})
                    log(f"Updated '{upd_set_col}' to '{upd_set_val.strip()}' for records in '{upd_tbl}' where {upd_find_col} = '{upd_find_val.strip()}'.", "ok")

        show_log()
        st.markdown('</div>', unsafe_allow_html=True)


    # ── DELETE ─────────────────────────────────────────────────────────────
    with q4:
        st.markdown('<div class="section-card card-plum">', unsafe_allow_html=True)
        st.markdown("### ✕ Delete Records")
        st.markdown('<p class="hint-text">Identify which records to delete. This action cannot be undone.</p>', unsafe_allow_html=True)

        del_db, del_tbl, del_cols = db_table_selectors("del")

        if del_cols:
            del_find_col = st.selectbox("Delete records where this field…", del_cols, key="del_find_col")
            del_find_val = st.text_input("…equals", key="del_find_val", placeholder="Enter value to match…")
            confirm_del  = st.checkbox("Yes, permanently delete these records", key="confirm_del")

            if st.button("Delete Records →", key="btn_delete"):
                if not del_find_val.strip():
                    log("Please specify which records to delete.", "error")
                elif not confirm_del:
                    log("Please confirm before deleting.", "warn")
                else:
                    where = {del_find_col: del_find_val.strip()}
                    # TODO: requests.delete("http://localhost:8080/query/delete", json={
                    #     "db": del_db, "table": del_tbl, "where": where})
                    log(f"Deleted records from '{del_tbl}' in '{del_db}' where {del_find_col} = '{del_find_val.strip()}'.", "ok")

        show_log()
        st.markdown('</div>', unsafe_allow_html=True)


