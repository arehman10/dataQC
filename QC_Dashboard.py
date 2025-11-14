# qc_dashboard.py
#
# WBES / B-READY Data QC Dashboard + AI Co-Pilot
# -----------------------------------------------
# - Upload a consolidated QC Excel file (out_consolidated_QC_....xlsx)
# - Get:
#     • High-level QC health dashboard (counts & charts)
#     • Interview triage (QC score, tiers, priorities)
#     • Question triage (low informative / skip / invalid)
#     • Issue detail tabs (Outliers, Productivity, GPS, strings, etc.)
#     • Check dictionary
#     • AI Co-Pilot: summaries, Q&A, vendor email drafts (if OPENAI_API_KEY set)
#
# Notes:
# - Column and sheet names are aligned to the Sri Lanka QC file you shared.
# - All counts, averages, and charts are based directly on those sheets.

# ---------- Imports & AI client setup ----------

import io
import os
import ssl

import certifi
import httpx
import numpy as np
import pandas as pd
import streamlit as st

try:
    from openai import OpenAI
except Exception:
    OpenAI = None

AI_CLIENT = None
HTTP_TIMEOUT = 600  # seconds; tweak if you like
INTERVIEWER_VAR = "a12"  # interviewer code in CONSOLIDATED_by_interview


@st.cache_resource
def create_openai_client(api_key: str | None):
    """
    Create and cache an OpenAI client using a custom httpx.Client and SSL context.
    Returns None if OpenAI or api_key is missing.
    """
    if OpenAI is None or not api_key:
        return None

    try:
        ctx = ssl.create_default_context(cafile=certifi.where())
        http_client = httpx.Client(
            verify=ctx,
            timeout=HTTP_TIMEOUT,
            follow_redirects=True,
        )
        client = OpenAI(api_key=api_key, http_client=http_client)
        return client
    except Exception:
        return None


def ai_available_text() -> str:
    if AI_CLIENT is None:
        return (
            "AI features are currently **disabled**.\n\n"
            "Enter an API key in the sidebar (or set OPENAI_API_KEY) to enable them."
        )
    return "AI features are **enabled** (OpenAI client is configured)."
AI_ENABLED = AI_CLIENT is not None
# ---------- Config ----------

# Optional: if you often use a specific file locally, you can set this:
DEFAULT_QC_PATH = None  # e.g. "out_consolidated_QC_Sri Lanka_2025-11-13.xlsx"

REQUIRED_SHEETS = [
    "CONSOLIDATED_by_interview",
    "CONSOLIDATED_by_question",
]

# Interview-level issue columns (from your Sri Lanka file)
MAJOR_ISSUE_COLS = [
    "OUTLIERS_by_interview",
    "LOGIC_CHECKS_by_interview",
    "PRODUCTIVITY_by_interview",
    "GPS_by_interview",
    "BR_OUTLIERS_by_interview",
    "REST_OUTLIERS_by_interview",
    "d2_d2x",
    "n3_n3x",
]

MINOR_ISSUE_COLS = [
    "SKIPS_by_interview",
    "INVALIDS_by_interview",
    "STRINGS_by_interview",
]

# ---------- Helpers ----------

def _parse_pct(series: pd.Series) -> pd.Series:
    """Convert '97%' / '100%' / '' into float 0–100."""
    s = series.astype(str).str.strip()
    s = s.str.replace("%", "", regex=False)
    s = s.replace({"": np.nan, "nan": np.nan, "NaN": np.nan})
    return pd.to_numeric(s, errors="coerce")

def _classify_check_type(text: str) -> str:
    """Heuristic classification of check type from check_explanations."""
    t = str(text)
    if t.startswith("Skip-Check"):
        return "Skip"
    if "Logic Check" in t:
        return "Logic"
    if "Range Check" in t:
        return "Range"
    return "Other"


def build_interviewer_summary(
    df_int: pd.DataFrame,
    interviewer_var: str = INTERVIEWER_VAR,
) -> pd.DataFrame:
    """
    Aggregate QC status by interviewer (a12).

    One row per interviewer with:
      - n_interviews
      - n_with_issues
      - pct_with_issues
      - avg_qc_score
      - avg_share_asked
      - avg_share_informative
      - counts by QC tier (A/B/C/D…)
    """
    if interviewer_var not in df_int.columns:
        return pd.DataFrame()

    df = df_int.copy()
    df[interviewer_var] = df[interviewer_var].astype(str)

    base = (
        df.groupby(interviewer_var)
        .agg(
            n_interviews=("idu", "size"),
            n_with_issues=("any_issue", lambda x: int(x.sum())),
            avg_qc_score=("qc_score", "mean"),
            avg_share_asked=("share_properly_asked_answered_num", "mean"),
            avg_share_informative=("share_proper_informative_num", "mean"),
        )
        .reset_index()
        .rename(columns={interviewer_var: "interviewer"})
    )

    base["pct_with_issues"] = np.where(
        base["n_interviews"] > 0,
        100.0 * base["n_with_issues"] / base["n_interviews"],
        np.nan,
    )

    # QC tier distribution per interviewer – handle the case where qc_tier is missing / all NaN
    if "qc_tier" in df.columns and df["qc_tier"].notna().any():
        tier_table = pd.crosstab(df[interviewer_var].astype(str), df["qc_tier"])
        tier_table = tier_table.reset_index().rename(
            columns={interviewer_var: "interviewer"}
        )
        summary = base.merge(tier_table, on="interviewer", how="left")
    else:
        summary = base.copy()

    for col in ["avg_qc_score", "avg_share_asked", "avg_share_informative", "pct_with_issues"]:
        if col in summary.columns:
            summary[col] = summary[col].round(1)

    return summary



def _priority_rank(priority: str) -> int:
    mapping = {"High": 1, "Medium": 2, "Low": 3}
    return mapping.get(priority, 99)

@st.cache_data
def load_qc_excel(file_bytes: bytes) -> dict:
    """
    Load and preprocess QC Excel file.

    Returns a dict of DataFrames:
      - interview
      - question
      - outliers, rest_outliers, productivity, gps, strings, d2_d2x, n3_n3x, descriptions
      - checks (from check_explanations)
    """
    file_obj = io.BytesIO(file_bytes)
    xls = pd.ExcelFile(file_obj)
    sheets = xls.sheet_names

    missing = [s for s in REQUIRED_SHEETS if s not in sheets]
    if missing:
        raise ValueError(f"Missing required sheet(s): {', '.join(missing)}")

    # --- Interview-level consolidated sheet ---
    df_int = pd.read_excel(xls, "CONSOLIDATED_by_interview")

    # Ensure all issue columns exist
    for col in MAJOR_ISSUE_COLS + MINOR_ISSUE_COLS:
        if col not in df_int.columns:
            df_int[col] = np.nan

    # Convert percentages to numeric
    df_int["share_properly_asked_answered_num"] = _parse_pct(
        df_int["share_properly_asked_answered"]
    )
    df_int["share_proper_informative_num"] = _parse_pct(
        df_int["share_proper_informative"]
    )

    issue_cols = MAJOR_ISSUE_COLS + MINOR_ISSUE_COLS
    df_int["any_issue"] = df_int[issue_cols].notna().any(axis=1)
    df_int["num_issue_types"] = df_int[issue_cols].notna().sum(axis=1)

    # Compute QC score, tier, priority
    def _compute_qc(row: pd.Series) -> pd.Series:
        asked = row["share_properly_asked_answered_num"]
        inf = row["share_proper_informative_num"]
        if pd.isna(asked):
            asked = 0.0
        if pd.isna(inf):
            inf = 0.0

        n_major = sum(pd.notna(row[c]) for c in MAJOR_ISSUE_COLS)
        n_minor = sum(pd.notna(row[c]) for c in MINOR_ISSUE_COLS)

        score = 100.0
        # Soft penalties for low coverage / informativeness
        if asked < 98:
            score -= (98 - asked) * 0.5
        if inf < 90:
            score -= (90 - inf) * 0.7
        # Penalties for issue types
        score -= n_major * 5
        score -= n_minor * 2
        score = float(np.clip(score, 0, 100))

        total_issues = n_major + n_minor
        if score >= 90 and total_issues == 0:
            tier = "A – Excellent"
        elif score >= 80:
            tier = "B – Good"
        elif score >= 70:
            tier = "C – Needs attention"
        else:
            tier = "D – Problem interview"

        # Priority based on severity + tier
        if tier == "D – Problem interview" or (tier == "C – Needs attention" and n_major > 0):
            priority = "High"
        elif tier in {"B – Good", "C – Needs attention"}:
            priority = "Medium"
        else:
            priority = "Low"

        return pd.Series(
            {
                "qc_score": round(score, 1),
                "qc_tier": tier,
                "priority": priority,
                "priority_rank": _priority_rank(priority),
                "n_major_issues": n_major,
                "n_minor_issues": n_minor,
            }
        )

    qc_scores = df_int.apply(_compute_qc, axis=1)
    df_int = pd.concat([df_int, qc_scores], axis=1)

    # --- Question-level consolidated sheet ---
    df_q = pd.read_excel(xls, "CONSOLIDATED_by_question")
    df_q["response_rate_num"] = _parse_pct(df_q["response_rate"])
    df_q["response_rate_informative_num"] = _parse_pct(df_q["response_rate_informative"])
    df_q["module"] = (
        df_q["varname"].astype(str).str.extract(r"^([A-Za-z]+)")[0].str.upper()
    )

    # --- Helper to read optional sheets safely ---
    def _read_optional(name: str) -> pd.DataFrame:
        if name in sheets:
            return pd.read_excel(xls, name)
        return pd.DataFrame()

    df_outliers = _read_optional("OUTLIERS")
    df_rest = _read_optional("rest_OUTLIERS")
    df_prod = _read_optional("Productivity")
    df_gps = _read_optional("GPS")
    df_strings = _read_optional("strings")
    df_d2 = _read_optional("d2 d2x")
    df_n3 = _read_optional("n3 n3x")
    df_desc = _read_optional("descriptions")
    df_checks_raw = _read_optional("check_explanations")

    df_checks = pd.DataFrame()
    if not df_checks_raw.empty:
        # The file has a single, long header; rename to 'description'
        col0 = df_checks_raw.columns[0]
        df_checks = df_checks_raw.rename(columns={col0: "description"})
        df_checks = df_checks.dropna(subset=["description"])
        df_checks["type"] = df_checks["description"].apply(_classify_check_type)

    return {
        "interview": df_int,
        "question": df_q,
        "outliers": df_outliers,
        "rest_outliers": df_rest,
        "productivity": df_prod,
        "gps": df_gps,
        "strings": df_strings,
        "d2_d2x": df_d2,
        "n3_n3x": df_n3,
        "descriptions": df_desc,
        "checks": df_checks,
    }

# ---------- AI helpers ----------


def ask_ai(context: str, question: str, max_output_tokens: int = 25000) -> str:
    """
    Wrapper around the Responses API, using GPT-5.1 and the custom httpx client.
    Always expects `context` to contain as much QC data as possible
    (ideally the full QC dump across all sheets).
    """
    if AI_CLIENT is None:
        return "AI is not configured. Please enter an API key in the sidebar and retry."

    developer_instructions = (
        "You are a senior World Bank Enterprise Survey data-quality expert. "
        "You are given a QC_DUMP containing data from all QC sheets (interviews, questions, "
        "outliers, productivity, GPS, strings, etc.). "
        "All numeric facts, counts, and shares you report MUST be directly supported by QC_DUMP. "
        "If you cannot find an exact number in QC_DUMP, explicitly say that it is not available "
        "instead of guessing. Pay close attention to detail across all sheets. "
        "If the question is high-level, summarise patterns; if the question is precise, "
        "answer precisely or say you cannot tell from the provided data."
    )

    user_content = f"QC_DUMP:\n{context}\n\nUSER_QUESTION:\n{question}"

    resp = AI_CLIENT.responses.create(
        model="gpt-5.1",
        input=[
            {"role": "developer", "content": developer_instructions},
            {"role": "user", "content": user_content},
        ],
        store=True,
        max_output_tokens=max_output_tokens,
        reasoning={"effort": "medium"},
    )

    text = getattr(resp, "output_text", "") or ""
    return text.strip()


def build_global_qc_context(df_int: pd.DataFrame, df_q_dyn: pd.DataFrame) -> str:
    """Compact global QC context for the AI Co-Pilot."""
    lines = []

    n_interviews = len(df_int)
    n_with_issues = int(df_int["any_issue"].sum())
    avg_asked = df_int["share_properly_asked_answered_num"].mean()
    avg_inf = df_int["share_proper_informative_num"].mean()
    n_questions = len(df_q_dyn)
    n_q_flagged = int(df_q_dyn["qc_flag"].sum())

    lines.append(
        f"There are {n_interviews} interviews in the dataset; "
        f"{n_with_issues} have at least one QC issue."
    )
    lines.append(
        f"Average % properly asked: {avg_asked:.1f}%. "
        f"Average % informative answers: {avg_inf:.1f}%."
    )
    lines.append(
        f"There are {n_questions} questions, of which {n_q_flagged} are QC-flagged "
        f"(low informative rate or many skips/invalids)."
    )

    # Worst interviews by QC score
    worst_int = df_int.sort_values("qc_score").head(8)
    lines.append("\nWORST INTERVIEWS (by QC score):")
    for _, r in worst_int.iterrows():
        lines.append(
            f"- idu {r['idu']} (technicalid={r.get('technicalid','')}): "
            f"QC score {r['qc_score']}, tier {r['qc_tier']}, priority {r['priority']}; "
            f"properly asked {r['share_properly_asked_answered_num']}%, "
            f"informative {r['share_proper_informative_num']}%."
        )

    # Flagged questions
    flagged_q = df_q_dyn[df_q_dyn["qc_flag"]].copy()
    flagged_q = flagged_q.sort_values("response_rate_informative_num").head(10)
    lines.append("\nMOST PROBLEMATIC QUESTIONS:")
    for _, r in flagged_q.iterrows():
        lines.append(
            f"- var {r['varname']} (module {r['module']}): "
            f"informative rate {r['response_rate_informative_num']}%, "
            f"response rate {r['response_rate_num']}%."
        )

    return "\n".join(lines)

def build_interview_context(idu_value: str, data: dict) -> str:
    """Build a compact context for a single interview idu across all sheets."""
    df_int = data["interview"]
    idu_str = str(idu_value)
    row = df_int[df_int["idu"].astype(str) == idu_str]
    if row.empty:
        return f"No interview found with idu={idu_str}."

    r = row.iloc[0]
    interviewer_code = r.get(INTERVIEWER_VAR, "")
    lines = []
    header = f"Interview idu={r['idu']}, technicalid={r.get('technicalid', '')}"
    if interviewer_code != "":
        header += f", interviewer_code={interviewer_code}"
    header += "."
    lines.append(header)

    lines.append(
        f"Properly asked: {r['share_properly_asked_answered']} "
        f"({r['share_properly_asked_answered_num']}%). "
        f"Informative: {r['share_proper_informative']} "
        f"({r['share_proper_informative_num']}%)."
    )
    lines.append(
        f"QC score: {r['qc_score']}, tier: {r['qc_tier']}, "
        f"priority: {r['priority']}; "
        f"{r['n_major_issues']} major and {r['n_minor_issues']} minor issue types."
    )

    # Issue descriptions present on the interview sheet
    for col in MAJOR_ISSUE_COLS + MINOR_ISSUE_COLS:
        if col in r.index and pd.notna(r[col]):
            lines.append(f"{col}: {r[col]}")

    # Detail counts per issue sheet
    for key, label in [
        ("outliers", "Outlier rows"),
        ("rest_outliers", "Additional outliers"),
        ("productivity", "Productivity checks"),
        ("gps", "GPS checks"),
        ("strings", "Text/string checks"),
        ("d2_d2x", "d2 vs d2x inconsistencies"),
        ("n3_n3x", "n3 vs n3x inconsistencies"),
        ("descriptions", "Description/ISIC checks"),
    ]:
        df = data[key]
        if not df.empty and "idu" in df.columns:
            subset = df[df["idu"].astype(str) == idu_str]
            if not subset.empty:
                lines.append(f"{label}: {len(subset)} row(s) in the {key} sheet.")

    return "\n".join(lines)



def build_full_qc_context(data: dict, max_rows_per_sheet: int = 500) -> str:
    """
    Build a text dump of ALL QC sheets for the AI to inspect.
    For each sheet: show row/column counts and up to `max_rows_per_sheet` rows as CSV.
    Increase max_rows_per_sheet if your QC file is small and you want truly full coverage.
    """
    lines = []
    for name, df in data.items():
        if not isinstance(df, pd.DataFrame):
            continue

        lines.append(f"=== SHEET: {name} ===")
        lines.append(f"Rows: {len(df)}, Columns: {list(df.columns)}")

        if len(df) <= max_rows_per_sheet:
            csv = df.to_csv(index=False)
        else:
            csv = df.head(max_rows_per_sheet).to_csv(index=False)
            lines.append(f"... showing first {max_rows_per_sheet} rows only ...")

        lines.append(csv)
        lines.append("")

    return "\n".join(lines)

# ---------- Streamlit UI ----------

st.set_page_config(
    page_title="WBES Data QC Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("WBES Data QC Dashboard")
st.caption("Interactive QC triage + AI Co-Pilot for Enterprise Surveys")

# --- Sidebar: file & filters ---
with st.sidebar:
    st.header("Data & Filters")

    uploaded = st.file_uploader(
        "Upload QC Excel file (.xlsx)",
        type=["xlsx"],
        help="Use the consolidated QC export (out_consolidated_QC_...).",
    )

    file_bytes = None
    source_label = None

    if uploaded is not None:
        file_bytes = uploaded.read()
        source_label = uploaded.name
    elif DEFAULT_QC_PATH and os.path.exists(DEFAULT_QC_PATH):
        with open(DEFAULT_QC_PATH, "rb") as f:
            file_bytes = f.read()
        source_label = DEFAULT_QC_PATH

    if file_bytes is None:
        st.info("Upload a QC Excel file, or set DEFAULT_QC_PATH in the script.")
        st.stop()

    try:
        data = load_qc_excel(file_bytes)
    except Exception as e:
        st.error(f"Could not load QC file: {e}")
        st.stop()

    df_int = data["interview"]
    df_q = data["question"]

    st.success(f"Loaded QC file: {source_label}")

    # -------- Build / cache full QC context for AI --------
    if "full_qc_context" not in st.session_state:
        st.session_state["full_qc_context"] = build_full_qc_context(data)

    # -------- OpenAI API key input --------
    st.markdown("---")
    st.subheader("OpenAI API key")

    default_key = st.session_state.get("api_key", "")
    api_key_input = st.text_input(
        "Enter OpenAI API key",
        type="password",
        value=default_key,
        help="Key is kept only in this session; do not share this app publicly with a real key.",
    )

    if api_key_input:
        st.session_state["api_key"] = api_key_input

    # Fallback to environment variable if no key entered in UI
    api_key = st.session_state.get("api_key") or os.getenv("OPENAI_API_KEY")

    AI_CLIENT = create_openai_client(api_key)

    st.markdown("**AI status**")
    st.write(ai_available_text())

    # -------- Interview filters --------
    st.markdown("---")
    st.subheader("Interview filters")

    idu_options = ["All"] + sorted(df_int["idu"].astype(str).unique().tolist())
    selected_idu = st.selectbox(
        "Focus on interview (idu)",
        idu_options,
        index=0,
        key="focus_idu_sidebar",
    )

    only_flagged_interviews = st.checkbox(
        "Show only interviews with any issue",
        value=True,
        key="chk_only_flagged_interviews",
    )

    priority_filter = st.multiselect(
        "Priority filter",
        options=["High", "Medium", "Low"],
        default=["High", "Medium", "Low"],
        key="priority_filter_sidebar",
    )

    interviewer_filter = []
    if INTERVIEWER_VAR in df_int.columns:
        interviewer_vals = (
            df_int[INTERVIEWER_VAR]
            .dropna()
            .astype(str)
            .sort_values()
            .unique()
            .tolist()
        )


    # -------- Question QC threshold --------
    st.markdown("---")
    st.subheader("Question QC flags")

    min_inf_threshold = st.slider(
        "Flag questions if informative rate is below:",
        min_value=0,
        max_value=100,
        value=70,
        step=5,
        help="Used to highlight questions with low informative responses.",
        key="slider_min_inf_threshold",
    )


# Recompute dynamic question flags based on sidebar threshold
df_q_dyn = df_q.copy()
df_q_dyn["qc_flag"] = (
    (df_q_dyn["response_rate_informative_num"] < min_inf_threshold)
    | df_q_dyn["SKIPS_by_question"].notna()
    | df_q_dyn["INVALIDS_by_question"].notna()
)

# ---------- Tabs ----------
tab_dash, tab_int, tab_qtab, tab_details, tab_checks, tab_ai = st.tabs(
    [
        "Dashboard",
        "Interviews",
        "Questions",
        "Issue details",
        "Check dictionary",
        "AI Co-Pilot",
    ]
)

# ---------- Dashboard tab ----------
# ---------- Dashboard tab ----------
with tab_dash:
    st.subheader("Overall QC health")

    n_interviews = len(df_int)
    n_with_issues = int(df_int["any_issue"].sum())
    n_questions = len(df_q_dyn)
    n_q_flagged = int(df_q_dyn["qc_flag"].sum())

    avg_asked = df_int["share_properly_asked_answered_num"].mean()
    avg_informative = df_int["share_proper_informative_num"].mean()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("# interviews", n_interviews)
    c2.metric(
        "Avg. % properly asked",
        f"{avg_asked:.1f}%" if not np.isnan(avg_asked) else "n/a",
    )
    c3.metric(
        "Avg. % informative answers",
        f"{avg_informative:.1f}%" if not np.isnan(avg_informative) else "n/a",
    )
    c4.metric("# interviews with issues", n_with_issues)

    c5, c6 = st.columns(2)
    with c5:
        st.markdown("**Interview QC tiers (count)**")
        tier_counts = (
            df_int["qc_tier"].value_counts().sort_index().rename_axis("qc_tier")
        )
        st.bar_chart(tier_counts)

    with c6:
        st.markdown("**Question QC flags**")
        st.metric("# questions", n_questions)
        st.metric("# flagged questions", n_q_flagged)

    st.markdown("---")
    st.subheader("Interviews with at least one issue by type")

    issue_counts = {}
    for col in MAJOR_ISSUE_COLS + MINOR_ISSUE_COLS:
        if col in df_int.columns:
            issue_counts[col] = int(df_int[col].notna().sum())

    if issue_counts:
        issue_df = (
            pd.DataFrame(
                {"issue_type": list(issue_counts.keys()), "count": list(issue_counts.values())}
            )
            .sort_values("count", ascending=False)
            .set_index("issue_type")
        )
        st.bar_chart(issue_df)
    else:
        st.info("No issue columns found to summarise.")

    # --- Status by interviewer (a12) ---
    st.markdown("---")
    st.subheader("Status by interviewer (a12)")

    if INTERVIEWER_VAR in df_int.columns:
        # Respect interviewer filter from sidebar if any
        if interviewer_filter:
            df_int_for_intv = df_int[df_int[INTERVIEWER_VAR].astype(str).isin(interviewer_filter)]
        else:
            df_int_for_intv = df_int

        interviewer_summary = build_interviewer_summary(
            df_int_for_intv,
            interviewer_var=INTERVIEWER_VAR,
        )

        if interviewer_summary.empty:
            st.info("No interviewer information (a12) available in the QC file (or no data after filters).")
        else:
            n_intv = len(interviewer_summary)
            max_top = min(30, n_intv)
            default_top = min(10, max_top)

            c_top, c_sort = st.columns([1, 2])
            with c_top:
                top_n = st.number_input(
                    "Top interviewers to display",
                    min_value=1,
                    max_value=max_top,
                    value=default_top,
                    step=1,
                    key="top_n_interviewers",
                )
            with c_sort:
                sort_option = st.selectbox(
                    "Sort interviewers by",
                    [
                        "Worst average QC score",
                        "Highest % interviews with issues",
                        "Most interviews completed",
                    ],
                    key="sort_interviewers_by",
                )

            interviewer_sorted = interviewer_summary.copy()
            if sort_option == "Worst average QC score" and "avg_qc_score" in interviewer_sorted.columns:
                interviewer_sorted = interviewer_sorted.sort_values("avg_qc_score", ascending=True)
            elif sort_option == "Highest % interviews with issues" and "pct_with_issues" in interviewer_sorted.columns:
                interviewer_sorted = interviewer_sorted.sort_values("pct_with_issues", ascending=False)
            elif "n_interviews" in interviewer_sorted.columns:
                interviewer_sorted = interviewer_sorted.sort_values("n_interviews", ascending=False)

            interviewer_top = interviewer_sorted.head(int(top_n))

            c_table, c_chart = st.columns([3, 2])

            with c_table:
                # Desired order, but filter to columns that actually exist
                preferred_cols = [
                    "interviewer",
                    "n_interviews",
                    "n_with_issues",
                    "pct_with_issues",
                    "avg_qc_score",
                    "avg_share_asked",
                    "avg_share_informative",
                ]
                preferred_cols = [c for c in preferred_cols if c in interviewer_top.columns]

                other_cols = [
                    c for c in interviewer_top.columns
                    if c not in preferred_cols
                ]

                cols_for_display = preferred_cols + other_cols

                st.markdown("**Per-interviewer QC summary**")
                st.dataframe(
                    interviewer_top[cols_for_display],
                    use_container_width=True,
                )

                # Download full summary
                full_cols_for_download = [
                    c for c in cols_for_display if c in interviewer_summary.columns
                ]
                csv_intv = interviewer_summary[full_cols_for_download].to_csv(
                    index=False
                ).encode("utf-8")
                st.download_button(
                    label="Download interviewer QC summary as CSV",
                    data=csv_intv,
                    file_name="qc_interviewer_summary.csv",
                    mime="text/csv",
                    key="download_interviewer_summary",
                )

            with c_chart:
                if "avg_qc_score" in interviewer_top.columns:
                    st.markdown("**Average QC score by interviewer**")
                    chart_data = interviewer_top.set_index("interviewer")[["avg_qc_score"]]
                    st.bar_chart(chart_data)
                else:
                    st.info("Average QC score not available for chart.")
    else:
        st.info("Column a12 (interviewer code) is not present in CONSOLIDATED_by_interview sheet.")



with tab_int:
    st.subheader("Interview-level QC triage")

    view = df_int.copy()

    # Filter by interviewer (a12) if selected
    if INTERVIEWER_VAR in view.columns and interviewer_filter:
        view = view[view[INTERVIEWER_VAR].astype(str).isin(interviewer_filter)]

    if only_flagged_interviews:
        view = view[view["any_issue"]]

    if priority_filter:
        view = view[view["priority"].isin(priority_filter)]

    if selected_idu != "All":
        view = view[view["idu"].astype(str) == selected_idu]

    cols_to_show = [
        "idu",
        "technicalid",
        "share_properly_asked_answered",
        "share_proper_informative",
        "qc_score",
        "qc_tier",
        "priority",
        "num_issue_types",
        "n_major_issues",
        "n_minor_issues",
        "OUTLIERS_by_interview",
        "LOGIC_CHECKS_by_interview",
        "PRODUCTIVITY_by_interview",
        "GPS_by_interview",
        "STRINGS_by_interview",
        "BR_OUTLIERS_by_interview",
        "REST_OUTLIERS_by_interview",
        "VENDOR_COMMENTS",
    ]

    # Insert interviewer code (a12) right after idu if present
    if INTERVIEWER_VAR in view.columns and INTERVIEWER_VAR not in cols_to_show:
        cols_to_show.insert(1, INTERVIEWER_VAR)

    # Final safety: only keep columns that actually exist in the filtered view
    cols_to_show = [c for c in cols_to_show if c in view.columns]

    if view.empty:
        st.info("No interviews match the current filters.")
    else:
        view_sorted = view.sort_values(
            ["priority_rank", "qc_score"], ascending=[True, True]
        )
        st.dataframe(view_sorted[cols_to_show], use_container_width=True)

        csv = view_sorted[cols_to_show].to_csv(index=False).encode("utf-8")
        st.download_button(
            label="Download filtered interviews as CSV",
            data=csv,
            file_name="qc_interviews_filtered.csv",
            mime="text/csv",
        )


# ---------- Issue details tab ----------
with tab_details:
    st.subheader("Detailed issues by sheet")
    st.markdown("Filters from the sidebar apply here too (especially the selected `idu`).")

    sub_out, sub_prod, sub_gps, sub_str, sub_d2, sub_n3, sub_rest, sub_desc = st.tabs(
        [
            "Outliers",
            "Productivity",
            "GPS",
            "Strings / text",
            "d2 vs d2x",
            "n3 vs n3x",
            "Other outliers",
            "Descriptions / ISIC",
        ]
    )

    def _filter_by_idu(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        if "idu" not in df.columns:
            return df
        if selected_idu == "All":
            return df
        return df[df["idu"].astype(str) == selected_idu]

    with sub_out:
        df = _filter_by_idu(data["outliers"])
        if df.empty:
            st.info("No outliers in this sheet (or sheet missing).")
        else:
            st.dataframe(df, use_container_width=True)

    with sub_prod:
        df = _filter_by_idu(data["productivity"])
        if df.empty:
            st.info("No productivity issues (or sheet missing).")
        else:
            st.dataframe(df, use_container_width=True)

    with sub_gps:
        df = _filter_by_idu(data["gps"])
        if df.empty:
            st.info("No GPS issues (or sheet missing).")
        else:
            st.dataframe(df, use_container_width=True)

    with sub_str:
        df = _filter_by_idu(data["strings"])
        if df.empty:
            st.info("No strings/text issues (or sheet missing).")
        else:
            st.dataframe(df, use_container_width=True)

    with sub_d2:
        df = _filter_by_idu(data["d2_d2x"])
        if df.empty:
            st.info("No d2/d2x inconsistencies (or sheet missing).")
        else:
            st.dataframe(df, use_container_width=True)

    with sub_n3:
        df = _filter_by_idu(data["n3_n3x"])
        if df.empty:
            st.info("No n3/n3x inconsistencies (or sheet missing).")
        else:
            st.dataframe(df, use_container_width=True)

    with sub_rest:
        df = _filter_by_idu(data["rest_outliers"])
        if df.empty:
            st.info("No additional outliers (or sheet missing).")
        else:
            st.dataframe(df, use_container_width=True)

    with sub_desc:
        df = _filter_by_idu(data["descriptions"])
        if df.empty:
            st.info("No description / ISIC issues (or sheet missing).")
        else:
            st.dataframe(df, use_container_width=True)

# ---------- Check dictionary tab ----------
with tab_checks:
    st.subheader("QC check dictionary")

    df_checks = data["checks"]
    if df_checks.empty:
        st.info("No check_explanations sheet or it is empty.")
    else:
        search = st.text_input(
            "Search checks (by text or type)", value=""
        )
        view_checks = df_checks.copy()
        if search:
            mask = view_checks["description"].str.contains(
                search, case=False, na=False
            )
            view_checks = view_checks[mask]

        cols = ["type", "description"]
        cols = [c for c in cols if c in view_checks.columns]
        st.dataframe(view_checks[cols], use_container_width=True)

# ---------- AI Co-Pilot tab ----------
# ---------- AI Co-Pilot tab ----------
with tab_ai:
    st.subheader("AI Co-Pilot for QC")

    if AI_CLIENT is None:
        st.info(ai_available_text())
    else:
        # Make sure full QC context is available once
        full_ctx = st.session_state.get("full_qc_context")
        if not full_ctx:
            full_ctx = build_full_qc_context(data)
            st.session_state["full_qc_context"] = full_ctx

        # --- Initialise session_state slots for AI outputs ---
        st.session_state.setdefault("ai_global_answer", "")
        st.session_state.setdefault("ai_single_summary", "")
        st.session_state.setdefault("ai_overall_summary", "")
        st.session_state.setdefault("ai_vendor_email", "")

        st.markdown(
            "Use the AI Co-Pilot to summarise issues, ask questions, or draft vendor emails. "
            "AI never changes the data; it only helps interpret it based on the QC file you uploaded."
        )

        # ----------------------------------------
        # 1. Global QC Q&A (free text)
        # ----------------------------------------
        st.markdown("### 1. Global QC Q&A")

        question_global = st.text_area(
            "Ask a question about the QC results",
            placeholder="e.g. How many interviews have productivity issues and what share of the sample is that?",
            key="global_qc_question",
        )

        if st.button("Ask AI about overall QC", key="btn_global_qc"):
            st.session_state["ai_global_answer"] = ask_ai(
                full_ctx,
                question_global or "Summarise the main QC issues across all sheets.",
                max_output_tokens=20000,
            )

        if st.session_state["ai_global_answer"]:
            st.markdown("**AI answer (based on the full QC dump):**")
            st.markdown(st.session_state["ai_global_answer"])

        st.markdown("---")

        # ----------------------------------------
        # 2. Per-interview AI summary
        # ----------------------------------------
        st.markdown("### 2. Per-interview AI summary")

        idu_for_summary = st.selectbox(
            "Select interview (idu) for AI summary",
            sorted(df_int["idu"].astype(str).unique().tolist()),
            key="idu_ai_summary",
        )

        if st.button("Generate AI summary for selected interview", key="btn_summary_one"):
            ctx_interview = build_interview_context(idu_for_summary, data)
            combined_ctx = full_ctx + "\n\n=== FOCUSED_INTERVIEW ===\n" + ctx_interview

            q = (
                "Summarise the data-quality issues for this interview and list "
                "3–5 concrete follow-up actions for the vendor or enumerator. "
                "Use the FOCUSED_INTERVIEW block, but you may also draw on the broader QC patterns."
            )
            st.session_state["ai_single_summary"] = ask_ai(combined_ctx, q, max_output_tokens=20000)

        if st.session_state["ai_single_summary"]:
            st.markdown("**AI summary for this interview:**")
            st.markdown(st.session_state["ai_single_summary"])

        st.markdown("---")

        # ----------------------------------------
        # 3. Overall summary for selected / all interviews
        # ----------------------------------------
        st.markdown("### 3. Overall summary for selected / all interviews")

        idu_multi_summary = st.multiselect(
            "Select interviews (idu) to include in the summary (leave blank and tick 'Use all' to summarise all interviews)",
            sorted(df_int["idu"].astype(str).unique().tolist()),
            key="idu_multi_summary",
        )

        use_all_for_summary = st.checkbox(
            "Use all interviews in the overall summary",
            value=False,
            key="chk_summary_all_interviews",
        )

        if st.button("Generate overall summary", key="btn_overall_summary"):
            if not idu_multi_summary and not use_all_for_summary:
                st.warning("Select at least one interview or tick 'Use all interviews'.")
            else:
                if use_all_for_summary:
                    selected_ids = sorted(df_int["idu"].astype(str).unique().tolist())
                else:
                    selected_ids = [str(x) for x in idu_multi_summary]

                parts = []
                for idu_val in selected_ids[:50]:  # safety cap
                    parts.append(f"INTERVIEW {idu_val}:\n" + build_interview_context(idu_val, data))
                focused_block = "\n\n".join(parts)

                combined_ctx = (
                    full_ctx
                    + "\n\n=== FOCUSED_INTERVIEWS ===\n"
                    + focused_block
                )

                q = (
                    "Provide an overall summary of the main data-quality issues across these interviews. "
                    "Highlight common patterns, potential enumerator/vendor problems, and 3–5 concrete recommendations "
                    "for improving data collection quality going forward."
                )
                st.session_state["ai_overall_summary"] = ask_ai(
                    combined_ctx, q, max_output_tokens=20000
                )

        if st.session_state["ai_overall_summary"]:
            st.markdown("**Overall summary:**")
            st.markdown(st.session_state["ai_overall_summary"])

        st.markdown("---")

        # ----------------------------------------
        # 4. Draft vendor email for selected / all interviews
        # ----------------------------------------
        st.markdown("### 4. Draft vendor email for selected / all interviews")

        idu_multi_email = st.multiselect(
            "Select interviews (idu) to include in the email (leave blank and tick 'Use all' to include all interviews)",
            sorted(df_int["idu"].astype(str).unique().tolist()),
            key="idu_multi_email",
        )

        use_all_for_email = st.checkbox(
            "Use all interviews in the vendor email",
            value=False,
            key="chk_email_all_interviews",
        )

        if st.button("Draft vendor email", key="btn_draft_email"):
            if not idu_multi_email and not use_all_for_email:
                st.warning("Select at least one interview or tick 'Use all interviews'.")
            else:
                if use_all_for_email:
                    selected_ids = sorted(df_int["idu"].astype(str).unique().tolist())
                else:
                    selected_ids = [str(x) for x in idu_multi_email]

                selected_subset = selected_ids[:50]  # safety cap
                parts = []
                for idu_val in selected_subset:
                    parts.append(f"INTERVIEW {idu_val}:\n" + build_interview_context(idu_val, data))
                focused_block = "\n\n".join(parts)

                combined_ctx = (
                    full_ctx
                    + "\n\n=== FOCUSED_INTERVIEWS_FOR_EMAIL ===\n"
                    + focused_block
                )

                q = (
                    "Draft a concise, professional email to the survey vendor summarising "
                    "the QC issues in these interviews and what they should do to correct them. "
                    "Use bullet points per interview, include interview IDs, and be neutral and operational. "
                    "Start with a short overall paragraph on QC patterns, then list interviews."
                )

                answer = ask_ai(combined_ctx, q, max_output_tokens=20000)
                # Save into session_state so it persists and is editable
                st.session_state["ai_vendor_email"] = answer
                st.session_state["email_draft_box"] = answer  # prefill editable field

        # Show the email editor whenever we have something in state
        if st.session_state["ai_vendor_email"]:
            st.markdown("**Draft email (editable):**")
            st.text_area(
                "You can copy-edit this before sending:",
                key="email_draft_box",
                height=380,
            )
            # Keep the canonical email text in sync with user edits
            st.session_state["ai_vendor_email"] = st.session_state["email_draft_box"]
