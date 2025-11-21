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

import zipfile
import tempfile
from pathlib import Path

import ssaw  # pip install ssaw

import certifi
import httpx
import numpy as np
import pandas as pd
import streamlit as st

import requests
import time
from zipfile import ZipFile
import tempfile
from pathlib import Path

try:
    from openai import OpenAI
except Exception:
    OpenAI = None


AI_CLIENT = None
HTTP_TIMEOUT = 600  # seconds; tweak if you like
INTERVIEWER_VAR = "a12"  # interviewer code in CONSOLIDATED_by_interview


def _extract_main_stata_from_zip(zip_path: str) -> Path:
    tmpdir = Path(tempfile.mkdtemp(prefix="suso_export_"))
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(tmpdir)

    dta_files = list(tmpdir.rglob("*.dta"))
    if not dta_files:
        raise FileNotFoundError(f"No .dta files found in export zip: {zip_path}")

    return _select_main_dta(dta_files)




def download_raw_from_suso(
    client: ssaw.Client,
    questionnaire_id: str,
    questionnaire_version: int,
    interview_status: str = "All",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Use SSAW to download STATA export for a given questionnaire + interview_status,
    and return (raw_df, raw_df_labeled) dataframes.

    IMPORTANT:
      - We DO NOT use generate=True here because the ExportJob path in SSAW is
        currently buggy (QuestionnaireId field required).
      - Instead, we assume an export has already been generated in HQ for this
        questionnaire / version / status and just download the latest file.
    """
    from ssaw import ExportApi

    q_identity = f"{questionnaire_id}${questionnaire_version}"

    api = ExportApi(client)

    # Try to download latest existing export
    zip_filename = api.get(
        questionnaire_identity=q_identity,
        export_type="STATA",
        interview_status=interview_status,
        # DO NOT use generate=True here – that hits the buggy ExportJob path
        generate=False,
        show_progress=False,
    )

    if not zip_filename:
        # No export file was returned by SSAW. Give a clear error instead of
        # letting zipfile.ZipFile(None, ...) crash with "'NoneType' has no attribute 'seek'".
        raise RuntimeError(
            "Survey Solutions did not return any STATA export file for this "
            "questionnaire / version / status.\n\n"
            "Most likely causes:\n"
            "  • No export has been generated yet in HQ for this questionnaire/version/status.\n"
            "  • The HQ user or token does not have access to that export.\n\n"
            "Please go to HQ → Exports, create a STATA export for the same questionnaire, "
            "version, workspace, and interview status, wait for it to finish, and then try again."
        )

    # Now zip_filename should be a valid path to the downloaded zip
    main_dta = _extract_main_stata_from_zip(zip_filename)

    # Numeric version (for your QC engine)
    raw_df = pd.read_stata(main_dta, convert_categoricals=False)
    # Labeled version (for AI context)
    raw_df_labeled = pd.read_stata(main_dta, convert_categoricals=True)

    return raw_df, raw_df_labeled

def suso_export_to_stata(
    server: str,
    workspace: str,
    quest_id: str,
    version: int,
    work_status: str = "Completed",
    api_user: str | None = None,
    api_password: str | None = None,
    token: str | None = None,
    export_password: str | None = None,
    save_dir: str | None = None,   # <-- NEW optional argument
) -> tuple[pd.DataFrame, pd.DataFrame]:

    """
    Generate and download STATA export via Survey Solutions API,
    mirroring the behavior of R's suso_export / Stata's susoapi.

    Steps:
      1. POST /{workspace}/api/v2/export to start a STATA export job
      2. Poll /{workspace}/api/v2/export/{job_id} until ExportStatus == 'Completed'
      3. GET /{workspace}/api/v2/export/{job_id}/file to download the zip
      4. Unzip (with password if provided)
      5. Load the main .dta file twice:
         - numeric codes (for QC engine)
         - labeled version (for AI / human-readable context)

    Returns:
      (raw_df_numeric, raw_df_labeled)
    """
    server = server.rstrip("/")
    workspace = workspace.strip("/") or "primary"

    # --- Build QuestionnaireId correctly ---
    # quest_id might be:
    #   - bare GUID: "5a3a6a74-88d6-4a46-a3ef-b43005d1c9af"
    #   - GUID w/o dashes: "5a3a6a7488d64a46a3efb43005d1c9af"
    #   - full identity: "5a3a6a7488d64a46a3efb43005d1c9af$4"
    base_guid = quest_id.split("$")[0]          # take part before any $
    base_guid = base_guid.replace("-", "")      # remove dashes
    questionnaire_identity = f"{base_guid}${version}"

    # --- HTTP client with auth (token or user/password) ---
    session = requests.Session()
    session.headers.update({"Accept": "application/json"})

    if token:
        session.headers["Authorization"] = f"Bearer {token}"
    elif api_user and api_password:
        session.auth = (api_user, api_password)
    else:
        raise RuntimeError("You must provide either a token or api_user + api_password for SuSo.")

    export_base = f"{server}/{workspace}/api/v2/export"

    # --- 1. Start export job ---
    # This is equivalent to R's suso_export(job_type="STATA", QuestionnaireId=..., InterviewStatus=...)
    body = {
        "ExportType": "STATA",
        "QuestionnaireId": questionnaire_identity,
        "InterviewStatus": work_status or "All",
    }

    start_resp = session.post(export_base, json=body)
    if start_resp.status_code not in (200, 202, 201):
        raise RuntimeError(
            f"Failed to start export job. Status: {start_resp.status_code}, body: {start_resp.text}"
        )

    job_info = start_resp.json()
    job_id = job_info.get("JobId") or job_info.get("jobId")
    if not job_id:
        raise RuntimeError(f"Export job did not return JobId. Response: {start_resp.text}")

    # --- 2. Poll job status until completed ---
    status_url = f"{export_base}/{job_id}"
    while True:
        stat = session.get(status_url)
        stat.raise_for_status()
        info = stat.json()
        status = info.get("ExportStatus") or info.get("exportStatus")

        if status == "Completed":
            break
        if status in {"Fail", "Failed", "Faulted", "Canceled"}:
            raise RuntimeError(f"Export job {job_id} failed: {info}")

        # Optional: progress = info.get("Progress") or info.get("progress")
        time.sleep(5)

    # --- 3. Download export zip file ---
    download_url = f"{export_base}/{job_id}/file"
    zresp = session.get(download_url)
    zresp.raise_for_status()

    tmpdir = Path(tempfile.mkdtemp(prefix="suso_export_"))
    zip_path = tmpdir / "export.zip"
    zip_path.write_bytes(zresp.content)

    # --- 4. Unzip (with password if needed) ---
    extract_dir = tmpdir / "unzipped"
    extract_dir.mkdir(exist_ok=True)

    try:
        with ZipFile(zip_path) as zf:
            if export_password:
                zf.extractall(path=extract_dir, pwd=export_password.encode("utf-8"))
            else:
                zf.extractall(path=extract_dir)
    except RuntimeError as e:
        # typical error if zip is password-protected but no/incorrect password was provided
        raise RuntimeError(
            f"Failed to extract export zip (missing or wrong password?). "
            f"Original error: {e}"
        )

    # --- 5. Find main .dta file and load ---
    # --- 5. Find main .dta file and load ---
    # --- 5. Find main .dta file and load ---
    dta_files = list(extract_dir.rglob("*.dta"))
    if not dta_files:
        raise RuntimeError(f"No .dta files found in export (unzipped at {extract_dir})")

    main_dta = _select_main_dta(dta_files)


    # --- OPTIONAL: save permanent copies of the zip and main .dta ---
    if save_dir:
        try:
            permanent = Path(save_dir).expanduser()
            permanent.mkdir(parents=True, exist_ok=True)

            # Copy the raw SuSo export zip
            zip_copy = permanent / f"suso_{questionnaire_identity}_{work_status}.zip"
            zip_copy.write_bytes(zip_path.read_bytes())

            # Copy the main .dta used for QC
            dta_copy = permanent / f"suso_{questionnaire_identity}_{work_status}_main.dta"
            dta_copy.write_bytes(main_dta.read_bytes())
        except Exception as e:
            # Do NOT break the QC flow if saving fails; just log a warning
            print(f"Warning: could not save export to {save_dir}: {e}")

    raw_df_numeric = pd.read_stata(main_dta, convert_categoricals=False)
    raw_df_labeled = pd.read_stata(main_dta, convert_categoricals=True)

    return raw_df_numeric, raw_df_labeled



def find_interview_guid_for_idu(raw_df: pd.DataFrame, idu_value: str | int) -> str | None:
    """
    Try to locate the Survey Solutions interview GUID (interview__id) for a given idu.
    We match idu against:
      - interview__id
      - interview__key
      - technicalid
    and then return the interview__id from the matching row.
    """
    if raw_df is None or raw_df.empty:
        return None

    idu_str = str(idu_value)

    # if idu already equals interview__id
    if "interview__id" in raw_df.columns:
        mask = raw_df["interview__id"].astype(str) == idu_str
        if mask.any():
            return str(raw_df.loc[mask, "interview__id"].iloc[0])

    for col in ["interview__key", "technicalid"]:
        if col in raw_df.columns:
            mask = raw_df[col].astype(str) == idu_str
            if mask.any() and "interview__id" in raw_df.columns:
                return str(raw_df.loc[mask, "interview__id"].iloc[0])

    return None

def push_ai_rejections_to_suso(
    client: ssaw.Client,
    ai_df_int: pd.DataFrame,
    raw_df: pd.DataFrame,
    use_ai: bool = True,
) -> tuple[list[str], list[tuple[str, str]]]:
    """
    For each interview with ai_reject_decision == 'Reject' (or reject_decision if !use_ai),
    call Survey Solutions API to reject the interview.

    Returns:
      (success_ids, failed[(idu, error_msg), ...])
    """
    from ssaw import InterviewsApi

    if client is None:
        raise RuntimeError("No Survey Solutions client configured.")

    if ai_df_int is None or ai_df_int.empty:
        raise RuntimeError("No AI QC summary available to push rejections from.")

    dec_col = "ai_reject_decision" if use_ai and "ai_reject_decision" in ai_df_int.columns else "reject_decision"
    reasons_col = "ai_reject_reasons" if use_ai and "ai_reject_reasons" in ai_df_int.columns else "reject_reasons"

    df_reject = ai_df_int[ai_df_int.get(dec_col, "") == "Reject"].copy()
    if df_reject.empty:
        return [], []

    api = InterviewsApi(client)
    success = []
    failed = []

    for _, row in df_reject.iterrows():
        idu = str(row["idu"])
        interview_guid = find_interview_guid_for_idu(raw_df, idu)

        if not interview_guid:
            failed.append((idu, "Could not map idu to interview__id"))
            continue

        reasons = str(row.get(reasons_col, "")).strip()
        comment = f"Rejected by WBES QC dashboard. AI issues: {reasons}" if reasons else "Rejected by WBES QC dashboard (AI QC)."

        try:
            # If you want HQ-level rejection, use api.hqreject(interviewid=...) instead.
            api.reject(interviewid=interview_guid, comment=comment)
            success.append(idu)
        except Exception as e:
            failed.append((idu, str(e)))

    return success, failed


@st.cache_data
def load_raw_stata_labeled(file_bytes: bytes) -> pd.DataFrame:
    """
    Load raw survey data from a Stata .dta file with value labels converted to strings.
    Use this only for AI context (not for numeric QC).
    """
    file_obj = io.BytesIO(file_bytes)
    df = pd.read_stata(file_obj, convert_categoricals=True)
    return df


def record_ai_usage(resp, label: str | None = None) -> None:
    """
    Extract input / output / reasoning tokens from a Responses API result
    and accumulate them in st.session_state.
    """
    if AI_CLIENT is None:
        return

    usage = getattr(resp, "usage", None)
    if usage is None:
        return

    # Compatible with the current OpenAI Python SDK
    input_tokens = getattr(usage, "input_tokens", 0) or 0
    output_tokens = getattr(usage, "output_tokens", 0) or 0
    reasoning_tokens = getattr(usage, "reasoning_tokens", 0) or 0

    totals = st.session_state.get("ai_usage_total") or {
        "input": 0,
        "output": 0,
        "reasoning": 0,
    }
    totals["input"] += input_tokens
    totals["output"] += output_tokens
    totals["reasoning"] += reasoning_tokens
    st.session_state["ai_usage_total"] = totals

    log = st.session_state.get("ai_usage_log") or []
    log.append(
        {
            "label": label or "",
            "model": getattr(resp, "model", ""),
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "reasoning_tokens": reasoning_tokens,
        }
    )
    st.session_state["ai_usage_log"] = log

def _select_main_dta(dta_files: list[Path]) -> Path:
    """
    Choose the 'main' .dta file from a list of extracted .dta files.

    Preference order:
      1) Any file whose name contains '2025' AND has interview__key.
      2) Any file whose name contains '2025' (first one).
      3) Any file with interview__key in its columns.
      4) Fallback: first .dta in the list.
    """
    if not dta_files:
        raise FileNotFoundError("No .dta files found in export.")

    # 1) Prefer 2025 in filename
    dta_2025 = [p for p in dta_files if "2025" in p.name]

    # helper to test interview__key
    def has_interview_key(path: Path) -> bool:
        try:
            df_head = pd.read_stata(path, convert_categoricals=False, nrows=5)
        except Exception:
            return False
        return "interview__key" in df_head.columns

    # 1a) 2025 + interview__key
    for path in dta_2025:
        if has_interview_key(path):
            return path

    # 1b) any 2025 file
    if dta_2025:
        return dta_2025[0]

    # 2) any file with interview__key
    for path in dta_files:
        if has_interview_key(path):
            return path

    # 3) fallback
    return dta_files[0]

# ---------- NEW: raw-data + codebook helpers ----------

@st.cache_data
def load_raw_stata(file_bytes: bytes) -> pd.DataFrame:
    """
    Load raw survey data from a Stata .dta file.
    We keep original numeric codes (no categorical conversion) so that
    range / codebook checks are easier.
    """
    file_obj = io.BytesIO(file_bytes)
    df = pd.read_stata(file_obj, convert_categoricals=False)
    return df


@st.cache_data
def load_codebook_excel(file_bytes: bytes) -> dict:
    """
    Load Global Codebook-style Excel.

    Expects at least:
      - 'codebook'
      - 'Global Logic Checks'
      - 'Logic Checks for THIS surv.ONLY'
    Returns dict with DataFrames keyed by sheet name.
    """
    file_obj = io.BytesIO(file_bytes)
    xls = pd.ExcelFile(file_obj)

    sheets = {}
    if "codebook" in xls.sheet_names:
        sheets["codebook"] = pd.read_excel(xls, "codebook")
    else:
        sheets["codebook"] = pd.DataFrame()

    if "Global Logic Checks" in xls.sheet_names:
        sheets["global_logic"] = pd.read_excel(xls, "Global Logic Checks")
    else:
        sheets["global_logic"] = pd.DataFrame()

    if "Logic Checks for THIS surv.ONLY" in xls.sheet_names:
        sheets["survey_logic"] = pd.read_excel(xls, "Logic Checks for THIS surv.ONLY")
    else:
        sheets["survey_logic"] = pd.DataFrame()

    return sheets


def _parse_var_range_simple(var_range: str):
    """
    Very simple parser for Var range strings in the Global Codebook.

    Supported:
      - comma-separated list of numeric codes, e.g. '0,1,2,3,-9,.' ('.' ignored)

    Returns:
      allowed_values: list[float] or None
      expr: original expression string (for display in QC output)
    """
    import re

    if not isinstance(var_range, str):
        return None, None

    expr = var_range.strip()
    if not expr:
        return None, None

    # Only digits, minus, comma, period, whitespace => treat as code list
    if re.fullmatch(r"[0-9\-\.,\s]+", expr):
        allowed = []
        for tok in expr.replace(" ", "").split(","):
            if tok == "" or tok == ".":
                continue
            try:
                allowed.append(float(tok))
            except ValueError:
                # Fall back to treating as opaque expression
                return None, expr
        return allowed, expr

    # For anything more complex (inrange(), etc.) we keep expr for display,
    # but we do NOT automatically enforce it in Python (TODO if you want).
    return None, expr

from pathlib import Path

def _select_main_dta(dta_files: list[Path]) -> Path:
    """
    Choose the 'main' .dta file from a list of extracted .dta files.

    Preference order:
      1) Any file whose NAME contains '2025' AND has column interview__key.
      2) Any file whose NAME contains '2025'.
      3) Any file with interview__key in its columns.
      4) Fallback: first .dta in the list.
    """
    if not dta_files:
        raise FileNotFoundError("No .dta files found in export.")

    # helper to test for interview__key and non-empty
    def has_interview_key(path: Path) -> bool:
        try:
            df_head = pd.read_stata(path, convert_categoricals=False, nrows=5)
        except Exception:
            return False
        return "interview__key" in df_head.columns

    # 1) 2025 in filename AND interview__key
    dta_2025 = [p for p in dta_files if "2025" in p.name]
    for path in dta_2025:
        if has_interview_key(path):
            return path

    # 2) any 2025 file
    if dta_2025:
        return dta_2025[0]

    # 3) any file with interview__key
    for path in dta_files:
        if has_interview_key(path):
            return path

    # 4) last resort
    return dta_files[0]


def run_raw_qc(
    df_raw: pd.DataFrame,
    codebook_df: pd.DataFrame | None,
    vars_subset: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Basic raw-data QC using the Global Codebook.

    For each selected variable:
      - Uses Var range (when parsable as code list) for range / code checks.
      - Computes missing / zero / negative counts and basic stats.

    Returns:
      qc_vars: per-variable QC summary
      qc_interviews: per-interview raw QC summary
      qc_issues: long panel of individual issues
    """
    if codebook_df is None or codebook_df.empty:
        cb = None
    else:
        cb = codebook_df.set_index("var", drop=False)

    id_cols = [c for c in ["interview__key", "interview__id", "technicalid", "assignment__id"] if c in df_raw.columns]
    primary_id = id_cols[0] if id_cols else None

    qc_var_rows = []
    issue_rows = []

    # Keep track of how many raw issues per interview (by row index)
    raw_issue_counts = pd.Series(0, index=df_raw.index, dtype="int64")

    for var in vars_subset:
        if var not in df_raw.columns:
            continue

        s = df_raw[var]
        is_numeric = pd.api.types.is_numeric_dtype(s)
        n = len(s)
        n_miss = int(s.isna().sum())
        n_nonmiss = n - n_miss

        n_zero = int((s == 0).sum()) if is_numeric else 0
        n_neg = int((s < 0).sum()) if is_numeric else 0

        vmin = float(s.min()) if is_numeric and n_nonmiss > 0 else np.nan
        vmax = float(s.max()) if is_numeric and n_nonmiss > 0 else np.nan
        vmean = float(s.mean()) if is_numeric and n_nonmiss > 0 else np.nan

        var_label = ""
        section = ""
        var_type = ""
        var_range_expr = ""
        allowed_vals = None

        if cb is not None and var in cb.index:
            row_cb = cb.loc[var]
            var_label = str(row_cb.get("Description \n(see questionnaire for exact wording)", "")).strip()
            section = str(row_cb.get("section", "")).strip()
            var_type = str(row_cb.get("Var type", "")).strip()
            allowed_vals, var_range_expr = _parse_var_range_simple(row_cb.get("Var range", ""))

        # Range / code checks from Var range (only for numeric + simple code lists)
        if allowed_vals is not None and is_numeric:
            mask_valid = s.isna() | s.isin(allowed_vals)
            mask_invalid = ~mask_valid
            n_out_range = int(mask_invalid.sum())

            if n_out_range > 0:
                for idx, val in s[mask_invalid].items():
                    raw_issue_counts.loc[idx] += 1
                    issue_rows.append(
                        {
                            "source": "codebook_range", 
                            "var": var,
                            "interview_index": idx,
                            "interview_id": df_raw.loc[idx, primary_id] if primary_id else None,
                            "issue_type": "Range / code",
                            "detail": f"{var}={val} not in allowed set ({var_range_expr})",
                        }
                    )
        else:
            n_out_range = 0

        qc_var_rows.append(
            {
                "var": var,
                "label": var_label,
                "section": section,
                "Var type": var_type,
                "Var range (raw)": var_range_expr,
                "n_obs": n,
                "n_missing": n_miss,
                "pct_missing": 100.0 * n_miss / n if n > 0 else np.nan,
                "n_zero": n_zero,
                "n_negative": n_neg,
                "min": vmin,
                "max": vmax,
                "mean": vmean,
                "n_out_of_range": n_out_range,
            }
        )

    qc_vars = pd.DataFrame(qc_var_rows)

    # Per-interview raw QC summary
    inter_rows = []
    for idx in df_raw.index:
        row = {"row_index": idx, "raw_qc_issue_count": int(raw_issue_counts.loc[idx])}
        if primary_id:
            row["interview_id"] = df_raw.loc[idx, primary_id]
        if "technicalid" in df_raw.columns:
            row["technicalid"] = df_raw.loc[idx, "technicalid"]
        inter_rows.append(row)

    qc_interviews = pd.DataFrame(inter_rows)

    qc_issues = pd.DataFrame(issue_rows)

    return qc_vars, qc_interviews, qc_issues

def detect_numeric_string_issues(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Detect inconsistencies between numeric fields and their 'spelled-out'
    text versions such as d2 vs d2x, n3 vs n3x, and key N-section variables
    (n2a/n2as, n2i/n2is, n7a/n7as) directly from raw data.

    This is a deterministic detector; AI (numeric-string check) then
    explains and summarises these same issues for you.
    """
    pairs = [
        ("d2", "d2x"),
        ("n3", "n3x"),
        ("n2a", "n2as"),
        ("n2i", "n2is"),
        ("n7a", "n7as"),
    ]

    rows = []
    for num_col, txt_col in pairs:
        if num_col not in df_raw.columns or txt_col not in df_raw.columns:
            continue

        s_num = pd.to_numeric(df_raw[num_col], errors="coerce")
        s_txt = df_raw[txt_col].astype(str)

        # Basic cleaning for text: strip spaces and thousand separators
        cleaned = s_txt.str.replace(",", "", regex=False).str.strip()
        parsed = pd.to_numeric(cleaned, errors="coerce")

        mask = s_num.notna() & parsed.notna()
        mismatch = mask & (~np.isclose(s_num, parsed, rtol=1e-6, atol=1e-6))

        for idx in df_raw.index[mismatch]:
            rows.append(
                {
                    "source": "numeric_string_rule",
                    "var": num_col,
                    "issue_type": "Numeric/text mismatch",
                    "detail": (
                        f"{num_col}={s_num.loc[idx]} vs "
                        f"{txt_col}='{s_txt.loc[idx]}' parsed as {parsed.loc[idx]}"
                    ),
                    "interview_index": int(idx),
                }
            )

    return pd.DataFrame(rows)


def compute_rejection_flags(df_int: pd.DataFrame) -> pd.DataFrame:
    df = df_int.copy()

    if "raw_qc_issue_count" not in df.columns:
        df["raw_qc_issue_count"] = 0
    if "n_major_issues" not in df.columns:
        df["n_major_issues"] = 0
    if "numeric_mismatch_count" not in df.columns:
        df["numeric_mismatch_count"] = 0

    def _classify(row: pd.Series) -> pd.Series:
        issues = int(row.get("raw_qc_issue_count", 0) or 0)
        major = int(row.get("n_major_issues", 0) or 0)
        num_mismatch = int(row.get("numeric_mismatch_count", 0) or 0)
        qc_score = row.get("qc_score", np.nan)
        qc_score = float(qc_score) if not pd.isna(qc_score) else np.nan
        tier = str(row.get("qc_tier") or "")

        reasons = []
        hard_flag = False

        # HARD rules: any numeric mismatch or any major issue => Reject
        if num_mismatch > 0:
            hard_flag = True
            reasons.append(f"{num_mismatch} numeric/text mismatches (e.g. d2/d2x, n2/n2x)")
        if major > 0 and not hard_flag:
            hard_flag = True
            reasons.append(f"{major} major QC issue(s) recorded")

        # Softer rules based on score
        if not hard_flag and (issues >= 10 or (not pd.isna(qc_score) and qc_score < 60)):
            hard_flag = True
            reasons.append(
                f"High raw QC issue count ({issues}) and/or very low QC score ({qc_score})"
            )

        if hard_flag:
            decision = "Reject"
        elif tier.startswith("D") or (tier.startswith("C") and issues > 0):
            decision = "Review"
        else:
            decision = "Keep"

        return pd.Series(
            {
                "reject_decision": decision,
                "reject_hard_flag": hard_flag,
                "reject_reasons": "; ".join(reasons),
            }
        )

    extra = df.apply(_classify, axis=1)
    df = pd.concat([df, extra], axis=1)
    return df

def build_qc_data_from_raw(
    raw_df: pd.DataFrame,
    codebook_data: dict | None,
) -> dict:
    """
    Build all QC structures (interview-level, question-level, and issue-level)
    directly from raw Stata data + Global Codebook.

    Returns a dict with:
      - interview: df_int (one row per interview)
      - question: df_q (one row per variable / question)
      - raw_qc_vars, raw_qc_interviews, raw_qc_issues
      - checks: logic checks from the codebook
      - plus empty placeholders for old Excel-based sheets (outliers, etc.)
    """

    if "d1a1a" in raw_df.columns:
        n_before = len(raw_df)
        raw_df = raw_df[raw_df["d1a1a"].notna()].copy()
        n_after = len(raw_df)
        n_dropped = n_before - n_after
        if n_dropped > 0:
            st.info(
                f"QC restricted to interviews with non-missing d1a1a "
                f"(Stata: d1a1a != . & d1a1a != .a). "
                f"Kept {n_after}, dropped {n_dropped}."
            )
    # ---- 1. Codebook and survey variables ----
    cb_df = (
        codebook_data.get("codebook")
        if codebook_data and "codebook" in codebook_data
        else pd.DataFrame()
    )

    if not cb_df.empty and "var" in cb_df.columns:
        survey_vars = [v for v in cb_df["var"].astype(str) if v in raw_df.columns]
    else:
        # Fallback: treat all non-technical columns as survey vars
        survey_vars = [
            c
            for c in raw_df.columns
            if c
            not in [
                "interview__key",
                "interview__id",
                "technicalid",
                "assignment__id",
            ]
        ]

    # ---- 2. Run raw QC using the codebook ----
    raw_qc_vars, raw_qc_interviews, raw_qc_issues = run_raw_qc(
        df_raw=raw_df,
        codebook_df=cb_df,
        vars_subset=survey_vars,
    )

    # ---- 2b. Add non-codebook numeric/text mismatches as issues ----
    num_str_issues = detect_numeric_string_issues(raw_df)
    if not num_str_issues.empty:
        # Mark codebook issues with a source, if not already
        if not raw_qc_issues.empty and "source" not in raw_qc_issues.columns:
            raw_qc_issues["source"] = "codebook_range"

        # Make sure numeric issues also have a source column
        if "source" not in num_str_issues.columns:
            num_str_issues["source"] = "numeric_string_rule"

        # Update per-interview raw issue counts
        if (
            "interview_index" in num_str_issues.columns
            and "row_index" in raw_qc_interviews.columns
        ):
            add_counts = num_str_issues["interview_index"].value_counts()
            raw_qc_interviews = raw_qc_interviews.set_index("row_index")
            raw_qc_interviews["raw_qc_issue_count"] = (
                raw_qc_interviews["raw_qc_issue_count"]
                .add(add_counts, fill_value=0)
                .astype(int)
            )
            raw_qc_interviews = raw_qc_interviews.reset_index()

        # Combine into a unified issues table
        raw_qc_issues = pd.concat([raw_qc_issues, num_str_issues], ignore_index=True)

    # ---- 3. Build interview-level QC (df_int) ----
    # Choose an ID column for 'idu'
    if "interview__key" in raw_df.columns:
        id_col = "interview__key"
    elif "interview__id" in raw_df.columns:
        id_col = "interview__id"
    elif "technicalid" in raw_df.columns:
        id_col = "technicalid"
    else:
        id_col = None

    df_int = raw_qc_interviews.copy()

    if "raw_qc_issue_count" not in df_int.columns:
        df_int["raw_qc_issue_count"] = 0

    if "interview_id" in df_int.columns:
        # run_raw_qc already set this to primary_id when available
        df_int = df_int.rename(columns={"interview_id": "idu"})
    else:
        if id_col:
            df_int["idu"] = raw_df[id_col].values
        else:
            df_int["idu"] = df_int["row_index"].astype(str)

    # Attach interviewer code if present
    if INTERVIEWER_VAR in raw_df.columns:
        df_int[INTERVIEWER_VAR] = raw_df[INTERVIEWER_VAR].values

    # Interview-level coverage: percentage of survey questions answered
    if survey_vars:
        notnull = raw_df[survey_vars].notna()
        answered_counts = notnull.sum(axis=1)
        total_q = len(survey_vars)
        share = 100.0 * answered_counts / total_q

        df_int["share_properly_asked_answered_num"] = share.values
        df_int["share_properly_asked_answered"] = (
            df_int["share_properly_asked_answered_num"].round(1).astype(str) + "%"
        )
        # For now, treat all non-missing as informative
        df_int["share_proper_informative_num"] = share.values
        df_int["share_proper_informative"] = (
            df_int["share_proper_informative_num"].round(1).astype(str) + "%"
        )

    else:
        df_int["share_properly_asked_answered_num"] = np.nan
        df_int["share_properly_asked_answered"] = ""
        df_int["share_proper_informative_num"] = np.nan
        df_int["share_proper_informative"] = ""

    # --- SAFETY: ensure raw_qc_issue_count exists before using it ---
    if "raw_qc_issue_count" not in df_int.columns:
        # If we somehow lost it upstream, treat as zero issues for now.
        df_int["raw_qc_issue_count"] = 0

    df_int["any_issue"] = df_int["raw_qc_issue_count"] > 0
    df_int["num_issue_types"] = np.where(df_int["any_issue"], 1, 0)
    df_int["n_major_issues"] = df_int["raw_qc_issue_count"]
    df_int["n_minor_issues"] = 0


    # Compute QC score, tier, priority
    def _compute_qc_row(row: pd.Series) -> pd.Series:
        asked = row.get("share_properly_asked_answered_num", 0.0)
        inf = row.get("share_proper_informative_num", 0.0)
        asked = 0.0 if pd.isna(asked) else float(asked)
        inf = 0.0 if pd.isna(inf) else float(inf)
        n_major = int(row.get("n_major_issues", 0) or 0)

        score = 100.0
        if asked < 98:
            score -= (98 - asked) * 0.5
        if inf < 90:
            score -= (90 - inf) * 0.7
        score -= n_major * 3.0
        score = float(np.clip(score, 0, 100))

        if score >= 90 and n_major == 0:
            tier = "A – Excellent"
        elif score >= 80:
            tier = "B – Good"
        elif score >= 70:
            tier = "C – Needs attention"
        else:
            tier = "D – Problem interview"

        if tier.startswith("D"):
            priority = "High"
        elif tier.startswith("C"):
            priority = "Medium"
        else:
            priority = "Low"

        return pd.Series(
            {
                "qc_score": round(score, 1),
                "qc_tier": tier,
                "priority": priority,
                "priority_rank": _priority_rank(priority),
            }
        )

    qc_scores = df_int.apply(_compute_qc_row, axis=1)
    df_int = pd.concat([df_int, qc_scores], axis=1)

    # Reject / review / keep
    df_int = compute_rejection_flags(df_int)

    # ---- Attach idu / technicalid to raw_qc_issues for easier display ----
    issues_joined = raw_qc_issues.copy()
    if (
        not issues_joined.empty
        and "interview_index" in issues_joined.columns
        and "row_index" in raw_qc_interviews.columns
    ):
        key_cols = ["row_index", "idu"]
        if "technicalid" in df_int.columns:
            key_cols.append("technicalid")

        issues_joined = issues_joined.merge(
            df_int[key_cols],
            left_on="interview_index",
            right_on="row_index",
            how="left",
        )
        issues_joined.drop(columns=["row_index"], inplace=True, errors="ignore")

    raw_qc_issues = issues_joined

        # ---- Recompute per-interview issue counts from raw_qc_issues ----
    if not raw_qc_issues.empty and "idu" in raw_qc_issues.columns:
        # Total issues (all types)
        issue_counts = (
            raw_qc_issues.groupby("idu")
            .size()
            .rename("raw_qc_issue_count")
        )

        # Major issues: for now treat all issues as major; you can restrict to certain types later
        major_issue_counts = (
            raw_qc_issues.groupby("idu")
            .size()
            .rename("n_major_issues")
        )

        df_int = df_int.drop(columns=["raw_qc_issue_count"], errors="ignore")
        df_int = df_int.drop(columns=["n_major_issues"], errors="ignore")

        df_int = df_int.merge(issue_counts, on="idu", how="left")
        df_int = df_int.merge(major_issue_counts, on="idu", how="left")

        df_int["raw_qc_issue_count"] = (
            df_int["raw_qc_issue_count"].fillna(0).astype(int)
        )
        df_int["n_major_issues"] = (
            df_int["n_major_issues"].fillna(0).astype(int)
        )
    else:
        # If we truly have no issues, keep counts at 0
        if "raw_qc_issue_count" not in df_int.columns:
            df_int["raw_qc_issue_count"] = 0
        if "n_major_issues" not in df_int.columns:
            df_int["n_major_issues"] = 0


   # ---- 4. Build question-level QC (df_q) from raw_qc_vars ----
   # ---- 4. Build question-level QC (df_q) from raw_qc_vars ----
    df_q = raw_qc_vars.copy() if raw_qc_vars is not None else pd.DataFrame()

    # Ensure we have a varname column
    if "varname" not in df_q.columns:
        if "var" in df_q.columns:
            df_q = df_q.rename(columns={"var": "varname"})
        else:
            # Last-resort fallback: use index as varname so app doesn't crash
            df_q["varname"] = df_q.index.astype(str)

    # Ensure pct_missing exists
    if "pct_missing" not in df_q.columns:
        if {"n_obs", "n_missing"}.issubset(df_q.columns):
            df_q["pct_missing"] = 100.0 * df_q["n_missing"] / df_q["n_obs"]
        else:
            df_q["pct_missing"] = np.nan

    df_q["response_rate_num"] = 100.0 - df_q["pct_missing"]
    df_q["response_rate_informative_num"] = df_q["response_rate_num"]
    df_q["response_rate"] = df_q["response_rate_num"].round(1).astype(str) + "%"
    df_q["response_rate_informative"] = (
        df_q["response_rate_informative_num"].round(1).astype(str) + "%"
    )

    df_q["module"] = (
        df_q["varname"].astype(str).str.extract(r"^([A-Za-z]+)")[0].str.upper()
    )

    # Placeholders for skips / invalids to keep downstream code happy
    if "SKIPS_by_question" not in df_q.columns:
        df_q["SKIPS_by_question"] = np.nan
    if "INVALIDS_by_question" not in df_q.columns:
        df_q["INVALIDS_by_question"] = np.nan


    # ---- 5. Build checks from codebook logic sheets ----
    df_checks = pd.DataFrame()
    if codebook_data:
        globals_df = codebook_data.get("global_logic", pd.DataFrame())
        survey_df = codebook_data.get("survey_logic", pd.DataFrame())
        parts = []
        if globals_df is not None and not globals_df.empty:
            temp = globals_df.copy()
            temp["scope"] = "Global"
            parts.append(temp)
        if survey_df is not None and not survey_df.empty:
            temp = survey_df.copy()
            temp["scope"] = "Survey-specific"
            parts.append(temp)
        if parts:
            df_checks = pd.concat(parts, ignore_index=True)

    # ---- 6. Return a data dict compatible with the rest of the app ----
    data = {
        "interview": df_int,
        "question": df_q,
        "raw_qc_vars": raw_qc_vars,
        "raw_qc_interviews": raw_qc_interviews,
        "raw_qc_issues": raw_qc_issues,
        "checks": df_checks,
        # Placeholders to keep old code from crashing if referenced
        "outliers": pd.DataFrame(),
        "rest_outliers": pd.DataFrame(),
        "productivity": pd.DataFrame(),
        "gps": pd.DataFrame(),
        "strings": pd.DataFrame(),
        "d2_d2x": pd.DataFrame(),
        "n3_n3x": pd.DataFrame(),
        "descriptions": pd.DataFrame(),
    }

    return data


def build_qc_excel_report(
    df_int: pd.DataFrame,
    df_q_dyn: pd.DataFrame,
    raw_qc_vars: pd.DataFrame | None,
    raw_qc_interviews: pd.DataFrame | None,
) -> io.BytesIO:
    """
    Build a multi-sheet Excel report for TTL + vendor.

    If AI QC summary is available (ai_df_int in session_state), use that
    in place of df_int and include AI issue tables as separate sheets.
    """
    buf = io.BytesIO()

    ai_df_int = st.session_state.get("ai_df_int")
    ai_stats = st.session_state.get("ai_qc_stats")

    if isinstance(ai_df_int, pd.DataFrame) and not ai_df_int.empty:
        base_int = ai_df_int
        use_ai = True
    else:
        base_int = df_int
        use_ai = False

    # Decide which reject/score columns to use
    reject_col = "ai_reject_decision" if use_ai and "ai_reject_decision" in base_int.columns else "reject_decision"
    reason_col = "ai_reject_reasons" if use_ai and "ai_reject_reasons" in base_int.columns else "reject_reasons"
    score_col = "ai_qc_score" if use_ai and "ai_qc_score" in base_int.columns else "qc_score"
    tier_col = "ai_qc_tier" if use_ai and "ai_qc_tier" in base_int.columns else "qc_tier"

    # Overview metrics
    n_interviews = len(base_int)
    n_reject = int((base_int.get(reject_col, "") == "Reject").sum())
    n_review = int((base_int.get(reject_col, "") == "Review").sum())
    avg_score = float(base_int.get(score_col, pd.Series([np.nan])).mean())

    overview_rows = [
        {"Metric": "# interviews", "Value": n_interviews},
        {"Metric": "# recommended REJECT (AI)" if use_ai else "# recommended REJECT", "Value": n_reject},
        {"Metric": "# recommended REVIEW (AI)" if use_ai else "# recommended REVIEW", "Value": n_review},
        {"Metric": "Avg QC score (AI)" if use_ai else "Avg QC score", "Value": avg_score},
    ]
    if use_ai and ai_stats:
        overview_rows.append(
            {"Metric": "# AI-flagged interviews (any issue)", "Value": ai_stats.get("n_ai_issues_any", "")}
        )
        for k, v in ai_stats.get("ai_issue_summary", {}).items():
            overview_rows.append({"Metric": f"AI issues – {k}", "Value": v})

    df_overview = pd.DataFrame(overview_rows)

    # Recommended rejects table
    reject_cols = [
        c
        for c in [
            "idu",
            "technicalid",
            INTERVIEWER_VAR if INTERVIEWER_VAR in base_int.columns else None,
            score_col,
            tier_col,
            "priority" if "priority" in base_int.columns else None,
            reject_col,
            reason_col,
            "ai_issue_total" if use_ai and "ai_issue_total" in base_int.columns else None,
        ]
        if c is not None and c in base_int.columns
    ]
    df_rejects = base_int[base_int.get(reject_col, "") == "Reject"][reject_cols].copy()

    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        df_overview.to_excel(writer, sheet_name="Overview", index=False)
        base_int.to_excel(writer, sheet_name="Interviews_all", index=False)
        df_q_dyn.to_excel(writer, sheet_name="Questions_all", index=False)
        df_rejects.to_excel(writer, sheet_name="Recommended_rejects", index=False)

        if raw_qc_vars is not None and not raw_qc_vars.empty:
            raw_qc_vars.to_excel(writer, sheet_name="Raw_QC_by_var", index=False)
        if raw_qc_interviews is not None and not raw_qc_interviews.empty:
            raw_qc_interviews.to_excel(writer, sheet_name="Raw_QC_by_interview", index=False)

        # Extra AI sheets if available
        if use_ai and ai_stats:
            pd.DataFrame(
                [
                    {"stat": "n_interviews", "value": ai_stats["n_interviews"]},
                    {"stat": "n_ai_issues_any", "value": ai_stats["n_ai_issues_any"]},
                    {"stat": "n_ai_reject", "value": ai_stats["n_ai_reject"]},
                    {"stat": "n_ai_review", "value": ai_stats["n_ai_review"]},
                    {"stat": "avg_ai_qc_score", "value": ai_stats["avg_ai_qc_score"]},
                ]
            ).to_excel(writer, sheet_name="AI_QC_summary", index=False)

        for key, sheet_name in [
            ("ai_numeric_string_results", "AI_numeric_string"),
            ("ai_string_qc_results", "AI_string_QC"),
            ("ai_skip_qc_results", "AI_skip_checks"),
            ("ai_isic_qc_results", "AI_ISIC_QC"),
            ("ai_innovation_qc_results", "AI_innovation_QC"),
        ]:
            tbl = st.session_state.get(key)
            if isinstance(tbl, pd.DataFrame) and not tbl.empty:
                tbl.to_excel(writer, sheet_name=sheet_name, index=False)

    buf.seek(0)
    return buf



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


def get_primary_interview_id(row_df: pd.DataFrame) -> str:
    """
    Derive a single InterviewID for display from a 1-row raw_df slice.
    Priority: interview__id > interview__key > technicalid.
    """
    if row_df is None or row_df.empty:
        return ""
    row = row_df.iloc[0]
    for col in ["interview__id", "interview__key", "technicalid"]:
        if col in row_df.columns:
            val = row.get(col)
            if pd.notna(val):
                return str(val)
    return ""


def get_raw_rows_for_idu(raw_df: pd.DataFrame, idu_value: str | int) -> pd.DataFrame:
    """
    Return the raw Stata row(s) corresponding to a given interview idu.

    It tries common ID columns first (interview__key, interview__id, technicalid,
    assignment__id). If nothing matches, it falls back to treating idu_value as
    a row index.

    Always returns a DataFrame (possibly empty).
    """
    if raw_df is None or raw_df.empty:
        return raw_df.iloc[0:0].copy()

    idu_str = str(idu_value)

    # Try to match against usual ID columns
    for col in ["interview__key", "interview__id", "technicalid", "assignment__id"]:
        if col in raw_df.columns:
            sub = raw_df[raw_df[col].astype(str) == idu_str]
            if not sub.empty:
                return sub.copy()

    # Fallback: treat idu as row index
    try:
        idx = int(idu_str)
        if idx in raw_df.index:
            return raw_df.loc[[idx]].copy()
    except Exception:
        pass

    # No match found -> empty DF with same columns
    return raw_df.iloc[0:0].copy()


def build_ai_qc_summary(df_int: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Build an AI-enhanced interview-level QC summary.

    Uses AI result tables in st.session_state:
      - ai_numeric_string_results
      - ai_string_qc_results
      - ai_skip_qc_results
      - ai_isic_qc_results
      - ai_innovation_qc_results

    Returns:
      ai_df   : df_int + AI columns (ai_*), incl. ai_qc_score, ai_reject_decision, ai_reject_reasons.
      stats   : small dict with aggregate AI stats for dashboard/report.
    """
    ai_df = df_int.copy()
    ai_df["idu_str"] = ai_df["idu"].astype(str)
    ai_df = ai_df.set_index("idu_str", drop=False)

    def _map_counts(table_key: str, issue_mask_func) -> pd.Series:
        tbl = st.session_state.get(table_key)
        if not isinstance(tbl, pd.DataFrame) or tbl.empty or "idu" not in tbl.columns:
            return pd.Series(dtype="int64")
        df = tbl.copy()
        df["idu"] = df["idu"].astype(str)
        mask = issue_mask_func(df)
        if mask.sum() == 0:
            return pd.Series(dtype="int64")
        return df[mask].groupby("idu").size()

    # 1) Numeric/text mismatches (Block 5)
    num_counts = _map_counts(
        "ai_numeric_string_results",
        lambda df: ~df["Notes"].str.contains(
            "No numeric/text mismatches detected", case=False, na=False
        ),
    )

    # 2) String issues (Block 8)
    str_counts = _map_counts(
        "ai_string_qc_results",
        lambda df: ~df["Notes"].str.contains(
            "No string issues detected in the sample", case=False, na=False
        ),
    )

    # 3) Skip/hard-check violations (Block 7)
    skip_counts = _map_counts(
        "ai_skip_qc_results",
        lambda df: ~df["Notes"].str.contains(
            "No skip or hard-check violations detected in the sample", case=False, na=False
        ),
    )

    # 4) ISIC inconsistencies (Block 9) – based on IssueType
    isic_counts = _map_counts(
        "ai_isic_qc_results",
        lambda df: df.get("IssueType", "")
                  .astype(str)
                  .str.upper()
                  .ne("")
                  & df.get("IssueType", "")
                      .astype(str)
                      .str.upper()
                      .ne("OK"),
    )

    # 5) Innovation conflicts (Block 6) – respondent YES vs AI NO/VAGUE
    innov_tbl = st.session_state.get("ai_innovation_qc_results")
    if (
        isinstance(innov_tbl, pd.DataFrame)
        and not innov_tbl.empty
        and "RespondentFlag" in innov_tbl.columns
        and "is_innovation" in innov_tbl.columns
        and "idu" in innov_tbl.columns
    ):
        df_innov = innov_tbl.copy()
        df_innov["idu"] = df_innov["idu"].astype(str)
        resp = df_innov["RespondentFlag"].astype(str).str.upper()
        ai_flag = df_innov["is_innovation"].astype(str).str.upper()
        mask_conflict = (resp == "YES") & (ai_flag.isin(["NO", "VAGUE"]))
        if mask_conflict.any():
            innov_counts = df_innov[mask_conflict].groupby("idu").size()
        else:
            innov_counts = pd.Series(dtype="int64")
    else:
        innov_counts = pd.Series(dtype="int64")

    # Map counts back to ai_df
    ai_df["ai_numeric_issues"] = ai_df.index.to_series().map(num_counts).fillna(0).astype(int)
    ai_df["ai_string_issues"] = ai_df.index.to_series().map(str_counts).fillna(0).astype(int)
    ai_df["ai_skip_issues"] = ai_df.index.to_series().map(skip_counts).fillna(0).astype(int)
    ai_df["ai_isic_issues"] = ai_df.index.to_series().map(isic_counts).fillna(0).astype(int)
    ai_df["ai_innov_conflicts"] = ai_df.index.to_series().map(innov_counts).fillna(0).astype(int)

    ai_df["ai_issue_total"] = (
        ai_df["ai_numeric_issues"]
        + ai_df["ai_string_issues"]
        + ai_df["ai_skip_issues"]
        + ai_df["ai_isic_issues"]
        + ai_df["ai_innov_conflicts"]
    )

    def _compute_ai_row(row: pd.Series) -> pd.Series:
        base_score = float(row.get("qc_score", 100) or 100.0)

        penalty = (
            2 * int(row.get("ai_numeric_issues", 0) or 0)
            + 1 * int(row.get("ai_string_issues", 0) or 0)
            + 3 * int(row.get("ai_skip_issues", 0) or 0)
            + 3 * int(row.get("ai_isic_issues", 0) or 0)
            + 1 * int(row.get("ai_innov_conflicts", 0) or 0)
        )
        score = float(np.clip(base_score - penalty, 0, 100))

        total_ai = int(row.get("ai_issue_total", 0) or 0)

        # AI tier
        if score >= 90 and total_ai == 0:
            tier = "A – Excellent (AI)"
        elif score >= 80:
            tier = "B – Good (AI)"
        elif score >= 70:
            tier = "C – Needs attention (AI)"
        else:
            tier = "D – Problem interview (AI)"

        # AI-based decision
        hard = (
            int(row.get("ai_skip_issues", 0) or 0) > 0
            or int(row.get("ai_isic_issues", 0) or 0) > 0
            or int(row.get("ai_numeric_issues", 0) or 0) >= 2
        )

        reasons = []
        if int(row.get("ai_skip_issues", 0) or 0) > 0:
            reasons.append(f"{int(row['ai_skip_issues'])} skip/hard-check violations")
        if int(row.get("ai_isic_issues", 0) or 0) > 0:
            reasons.append(f"{int(row['ai_isic_issues'])} ISIC inconsistencies")
        if int(row.get("ai_numeric_issues", 0) or 0) > 0:
            reasons.append(f"{int(row['ai_numeric_issues'])} numeric/text mismatches")
        if int(row.get("ai_string_issues", 0) or 0) > 0:
            reasons.append(f"{int(row['ai_string_issues'])} string issues")
        if int(row.get("ai_innov_conflicts", 0) or 0) > 0:
            reasons.append(f"{int(row['ai_innov_conflicts'])} innovation flag conflicts")

        if hard:
            decision = "Reject"
        elif total_ai > 0 or str(row.get("reject_decision", "")).upper() == "REJECT":
            decision = "Review"
        else:
            decision = "Keep"

        return pd.Series(
            {
                "ai_qc_score": round(score, 1),
                "ai_qc_tier": tier,
                "ai_reject_decision": decision,
                "ai_reject_reasons": "; ".join(reasons),
            }
        )

    extra = ai_df.apply(_compute_ai_row, axis=1)
    ai_df = pd.concat([ai_df, extra], axis=1)

    # Clean index
    ai_df = ai_df.drop(columns=["idu_str"], errors="ignore")

    # Aggregate stats for dashboard/report
    stats = {}
    stats["n_interviews"] = len(ai_df)
    stats["n_ai_issues_any"] = int((ai_df["ai_issue_total"] > 0).sum())
    stats["n_ai_reject"] = int((ai_df["ai_reject_decision"] == "Reject").sum())
    stats["n_ai_review"] = int((ai_df["ai_reject_decision"] == "Review").sum())
    stats["avg_ai_qc_score"] = float(ai_df["ai_qc_score"].mean()) if "ai_qc_score" in ai_df.columns else None
    stats["ai_issue_summary"] = {
        "numeric": int(ai_df["ai_numeric_issues"].sum()),
        "string": int(ai_df["ai_string_issues"].sum()),
        "skip": int(ai_df["ai_skip_issues"].sum()),
        "isic": int(ai_df["ai_isic_issues"].sum()),
        "innov_conflicts": int(ai_df["ai_innov_conflicts"].sum()),
    }

    return ai_df, stats


def update_ai_qc_summary(df_int: pd.DataFrame) -> None:
    """
    Recompute AI QC summary and store in session_state:
      - ai_df_int
      - ai_qc_stats
    Safe to call after any AI QC block finishes.
    """
    try:
        ai_df, stats = build_ai_qc_summary(df_int)
        st.session_state["ai_df_int"] = ai_df
        st.session_state["ai_qc_stats"] = stats
    except Exception as e:
        st.warning(f"Could not recompute AI QC summary from AI QC tables: {e}")




def derive_respondent_innovation_flag(row_df: pd.DataFrame) -> str:
    """
    Derive whether the RESPONDENT said there is innovation.

    We look at common innovation flag variables (adjust list if needed):
      - 'innov'  (overall innovation flag)
      - 'h1', 'h5' (product/process innovation yes/no)

    Returns:
      'YES'       -> at least one flag is clearly yes (1 / 'yes' / 'y')
      'NO/OTHER'  -> flags observed but none clearly yes
      'UNKNOWN'   -> no flag variables or all missing
    """
    if row_df is None or row_df.empty:
        return "UNKNOWN"

    candidate_cols = [c for c in ["innov", "h1", "h5"] if c in row_df.columns]
    if not candidate_cols:
        return "UNKNOWN"

    any_obs = False
    any_yes = False

    for col in candidate_cols:
        val = row_df[col].iloc[0]
        if pd.isna(val):
            continue
        any_obs = True

        # numeric codes
        if isinstance(val, (int, float, np.integer, np.floating)):
            if int(val) == 1:
                any_yes = True
        else:
            s = str(val).strip().lower()
            if s in {"1", "yes", "y", "oui", "si"}:
                any_yes = True

    if not any_obs:
        return "UNKNOWN"
    return "YES" if any_yes else "NO/OTHER"


def highlight_innovation_rows(row: pd.Series) -> list[str]:
    """
    For Block 6 only:
      Highlight (red) when respondent said YES but AI said NO or VAGUE.

      RespondentFlag:
        - 'YES', 'NO/OTHER', 'UNKNOWN'
      is_innovation (AI):
        - 'YES', 'NO', 'VAGUE' (case-insensitive)

    All other combinations -> no highlight.
    """
    resp = str(row.get("RespondentFlag", "")).strip().upper()
    ai = str(row.get("is_innovation", "")).strip().upper()

    if resp == "YES" and ai in {"NO", "VAGUE"}:
        return ["background-color: #ffe5e5; font-weight: bold;" for _ in row]

    # everything else -> no styling
    return ["" for _ in row]


def parse_markdown_table(md: str, expected_first_headers: list[str]) -> list[dict]:
    """
    Generic markdown table parser.

    More tolerant:
      - Header row just needs to CONTAIN the expected headers (case-insensitive substring),
        not match them exactly.
      - Ignores stray lines; picks the first plausible header line.
    """
    if not isinstance(md, str):
        return []

    text = md.strip()
    if not text:
        return []

    lines = [ln.rstrip() for ln in md.splitlines() if ln.strip()]

    header_idx = None
    headers: list[str] = []

    for i, raw_ln in enumerate(lines):
        ln = raw_ln.strip()
        if not ln.startswith("|") or "|" not in ln:
            continue

        # split into cells
        cells = [c.strip() for c in ln.strip("|").split("|")]
        if len(cells) < 2:
            continue

        # check that each expected header appears in at least one cell (substring, case-insensitive)
        ok = True
        for h in expected_first_headers:
            h_lower = h.lower()
            if not any(h_lower in c.lower() for c in cells):
                ok = False
                break

        if not ok:
            continue

        # This looks like our header
        header_idx = i
        headers = cells
        break

    if header_idx is None or not headers:
        return []

    # Assume the separator row (---) is next; data rows after that
    data_lines = []
    if header_idx + 1 < len(lines) and set(lines[header_idx + 1].replace("|", "").strip()) <= {"-", " "}:
        # header, separator, then data
        data_lines = lines[header_idx + 2 :]
    else:
        # no explicit separator; treat following lines as data
        data_lines = lines[header_idx + 1 :]

    rows: list[dict] = []
    for raw_ln in data_lines:
        ln = raw_ln.strip()
        if not ln.startswith("|"):
            continue
        cells = [c.strip() for c in ln.strip("|").split("|")]
        if len(cells) != len(headers):
            # tolerate rows with trailing pipe + nothing -> pad
            if len(cells) < len(headers):
                cells = cells + [""] * (len(headers) - len(cells))
            else:
                continue

        row = dict(zip(headers, cells))
        rows.append(row)

    return rows


def highlight_issue_rows(row: pd.Series) -> list[str]:
    """
    Color the entire row red ONLY when there is an actual issue.

    Logic:
      1. If 'IssueType' column exists:
           - If IssueType is "" or "OK" (case-insensitive) -> no highlight.
           - Otherwise -> highlight whole row.
      2. Else (no IssueType):
           - Use Notes/report text:
             * If it contains a 'no issues' style phrase -> no highlight.
             * Otherwise -> highlight whole row.
    """
    # --- Case 1: use IssueType explicitly when available ---
    if "IssueType" in row.index:
        val = str(row["IssueType"]).strip().upper()
        if val == "" or val == "OK":
            # clean row -> no style
            return ["" for _ in row]
        else:
            # problematic row -> red + bold
            return ["background-color: #ffe5e5; font-weight: bold;" for _ in row]

    # --- Case 2: fall back to Notes/report text ---
    text_sources = []
    for col in ["Notes", "report", "Report"]:
        if col in row.index:
            text_sources.append(str(row[col]).lower())

    joined = " ".join(text_sources)

    ok_markers = [
        "no numeric/text mismatches detected",
        "no string issues detected in the sample",
        "no skip or hard-check violations detected in the sample",
        "no d1a1a/d1a1x/d1a2_v4 inconsistencies detected",
        "no d1a1a/d1a1x/d1a2_v4 inconsistencies detected in the sample",
        "no innovation descriptions detected in the sample",
        "no issue",
        "no issues",
    ]

    # if text indicates "no issue" -> no highlight
    if any(marker in joined for marker in ok_markers):
        return ["" for _ in row]

    # otherwise treat as an issue row
    return ["background-color: #ffe5e5; font-weight: bold;" for _ in row]





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

    df_int = compute_rejection_flags(df_int)

    # --- Question-level consolidated sheet ---
    df_q = pd.read_excel(xls, "CONSOLIDATED_by_question")
    df_q["response_rate_num"] = _parse_pct(df_q["response_rate"])
    df_q["response_rate_informative_num"] = _parse_pct(df_q["response_rate_informative"])
    df_q["module"] = (
        df_q["varname"].astype(str).str.extract(r"^([A-Za-z]+)")[0].str.upper()
    )
    # NEW: make sure question-level issue columns exist even if missing from the file
    for col in ["SKIPS_by_question", "INVALIDS_by_question"]:
        if col not in df_q.columns:
            df_q[col] = np.nan

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


def ask_ai(
    context: str,
    question: str,
    max_output_tokens: int = 85000,
    usage_label: str | None = None,
) -> str:
    """
    Wrapper around the Responses API, using GPT-5.1 and the custom httpx client.

    `context` should contain one or more CSV-like tables derived from:
      - the raw Stata survey data, and/or
      - QC summary tables (interview-level, question-level, issues), and/or
      - Global Codebook logic sheets.
    """
    if AI_CLIENT is None:
        return "AI is not configured. Please enter an API key in the sidebar and retry."

    developer_instructions = (
        "You are a senior data-quality expert for the World Bank Enterprise Surveys (WBES).\n"
        "You will receive one or more CSV tables and text blocks derived from the raw survey data "
        "and the Global Codebook. In these tables, each row typically represents either:\n"
        "- a single interview (interview-level table), or\n"
        "- a single variable/question (question-level table), or\n"
        "- a single issue or logic rule (issue/logic tables).\n\n"
        "Your primary job is to perform row-by-row data-quality checking based on the information provided. "
        "When you see interview-level data, go through the rows and look for:\n"
        "- out-of-range or implausible values (using codebook ranges if available),\n"
        "- inconsistencies between related fields (e.g. numeric vs spelled-out, totals vs components),\n"
        "- violations of skip logic and hard consistency rules described in the codebook logic sheets, and\n"
        "- suspicious strings or descriptions that conflict with numeric fields.\n\n"
        "All numeric facts, counts, and shares you report MUST be directly supported by these tables. "
        "Do NOT invent or guess values that are not in the data. If you cannot compute an exact number "
        "from the provided tables, say so explicitly and answer qualitatively instead.\n\n"
        "If the user asks a high-level question, summarise patterns across rows (for example, which kinds of "
        "issues are most common, which variables are most problematic, or which interviews look worst). "
        "If the user asks a precise question, either answer precisely from the tables or state clearly that "
        "the answer cannot be determined from the information provided."
    )

    # Generic context label; helpers decide what goes into `context`
    user_content = f"QC_CONTEXT:\n{context}\n\nUSER_QUESTION:\n{question}"

    resp = AI_CLIENT.responses.create(
        model="gpt-5.1",
        input=[
            {"role": "developer", "content": developer_instructions},
            {"role": "user", "content": user_content},
        ],
        store=True,
        max_output_tokens=max_output_tokens,
        reasoning={"effort": "high"},
    )

    record_ai_usage(resp, label=usage_label)

    text = getattr(resp, "output_text", "") or ""
    return text.strip()


def ai_check_innovation_strings(
    df_raw: pd.DataFrame,
    max_rows: int = 400,
) -> str:
    """
    Use AI (Oslo Manual 2005) to classify innovation descriptions.

    Output:
      - If descriptions exist: SINGLE markdown table:
            | InterviewID | is_innovation | TypeOfInnovation | Notes |
      - If none: EXACT line
            No innovation descriptions detected in the sample.
    """
    if AI_CLIENT is None:
        return "AI is not configured. Please enter an API key in the sidebar and retry."

    # Likely text fields where innovation descriptions live
    candidate_text_cols = ["h3x", "h6x", "d1a1x", "h3x_trans", "h6x_trans", "h32x", "h62x"]
    text_cols = [c for c in candidate_text_cols if c in df_raw.columns]

    if not text_cols:
        return "No innovation description text fields (e.g. h3x, h6x, d1a1x) found in the raw dataset."

    id_cols = [c for c in ["interview__key", "interview__id", "technicalid"] if c in df_raw.columns]
    flag_cols = [c for c in ["h1", "h5", "innov"] if c in df_raw.columns]

    cols = list(dict.fromkeys(id_cols + flag_cols + text_cols + ["d1a1a", "d1a1x"]))
    cols = [c for c in cols if c in df_raw.columns]

    df_subset = df_raw[cols].copy()
    # keep only rows with some description text
    df_subset = df_subset[df_subset[text_cols].notna().any(axis=1)]

    if df_subset.empty:
        return "No innovation descriptions detected in the sample."

    if len(df_subset) > max_rows:
        df_subset = df_subset.head(max_rows)

    csv_block = df_subset.to_csv(index=False)

    context = (
        "You are classifying firms' innovation descriptions using Oslo Manual (2005) definitions. "
        "You must decide if each description is an innovation and, if so, which type(s) of innovation.\n\n"
        "You receive a CSV where each row is one interview, including:\n"
        f"- ID columns (if present): {', '.join(id_cols) if id_cols else 'none'}\n"
        f"- Possible innovation flags: {', '.join(flag_cols) if flag_cols else 'none'}\n"
        f"- Description fields: {', '.join(text_cols)}\n"
        "- Optional: d1a1a and d1a1x (sector / main product).\n\n"
        "Use Oslo Manual definitions for product, process, organisational and marketing innovation "
        "(new to the firm is enough)."
        "\n\nCSV:\n"
        f"{csv_block}"
    )

    question = (
        "For EACH row with a non-empty description in the CSV:\n"
        "1. Decide is_innovation:\n"
        "   - YES: clearly an innovation under Oslo.\n"
        "   - NO: clearly not an innovation.\n"
        "   - VAGUE: description too vague to classify.\n"
        "2. Decide TypeOfInnovation:\n"
        "   - If innovation: one or more of product, process, marketing, organizational "
        "(separate by ';' if multiple).\n"
        "   - If not an innovation: NOT_INNOVATION.\n"
        "   - If vague: VAGUE.\n"
        "3. Notes: brief explanation (≤ 80 words) justifying your classification, referring to key phrases.\n\n"
        "InterviewID RULE:\n"
        "- Use interview__id if present; otherwise interview__key; otherwise technicalid; otherwise leave blank.\n\n"
        "OUTPUT FORMAT (VERY IMPORTANT):\n"
        "- Output ONLY a SINGLE markdown table with header:\n"
        "    | InterviewID | is_innovation | TypeOfInnovation | Notes |\n"
        "  and one row per interview.\n"
        "- Do NOT output any text before or after the table.\n"
        "- If there are NO rows with non-empty description, output EXACTLY this single line and nothing else:\n"
        "  No innovation descriptions detected in the sample."
    )

    return ask_ai(
        context,
        question,
        max_output_tokens=80000,
        usage_label="innovation_string_qc",
    )



def ai_check_skip_logic(
    raw_df: pd.DataFrame,
    codebook_data: dict | None,
    max_rows: int = 200,
) -> str:
    """
    Use AI to review skip logic and hard consistency checks defined in the
    Global Codebook sheets and look for obvious violations in a sample
    of the raw data.

    Output format:
      - If violations are found: a SINGLE markdown table with one row per violated rule.
      - If no violations are found: the exact line
            No skip or hard-check violations detected in the sample.
    """
    if AI_CLIENT is None:
        return "AI is not configured. Please enter an API key in the sidebar and retry."

    if not codebook_data:
        return "Global Codebook is not available in this session."

    df_global = codebook_data.get("global_logic", pd.DataFrame())
    df_survey = codebook_data.get("survey_logic", pd.DataFrame())

    if (df_global is None or df_global.empty) and (df_survey is None or df_survey.empty):
        return (
            "No 'Global Logic Checks' or 'Logic Checks for THIS surv.ONLY' sheets "
            "were found in the codebook. Cannot run skip/hard-check AI QC."
        )

    logic_parts = []
    if df_global is not None and not df_global.empty:
        logic_parts.append(
            "=== GLOBAL_LOGIC_SHEET_CSV ===\n" + df_global.to_csv(index=False)
        )
    if df_survey is not None and not df_survey.empty:
        logic_parts.append(
            "=== SURVEY_LOGIC_SHEET_CSV ===\n" + df_survey.to_csv(index=False)
        )

    logic_text = "\n\n".join(logic_parts)

    # Sample of raw data to keep context manageable
    sample = raw_df.head(max_rows)
    sample_csv = sample.to_csv(index=False)

    context = (
        "You are checking skip logic and hard consistency checks for a World Bank Enterprise Survey.\n"
        "You receive two inputs:\n"
        "1) LOGIC_SHEETS: logic-check tables from the Global Codebook describing skip patterns and hard rules.\n"
        "2) RAW_DATA_SAMPLE_CSV: a sample of the raw data (first {nrows} rows).\n\n"
        "LOGIC_SHEETS:\n"
        "{logic_text}\n\n"
        "RAW_DATA_SAMPLE_CSV (first {nrows} rows):\n"
        "{sample_csv}"
    ).format(
        nrows=len(sample),
        logic_text=logic_text,
        sample_csv=sample_csv,
    )

    question = (
        "Using the LOGIC_SHEETS and the RAW_DATA_SAMPLE_CSV, identify ONLY logic rules "
        "(skip logic or hard checks) that show clear violations in the sample.\n\n"
        "For each violated rule, produce ONE row in a markdown table with the following columns:\n"
        "| RuleSource | RuleLabel | RuleType | Variables | ApproxViolations | ExampleInterviewIDs | Notes |\n\n"
        "Where:\n"
        "- RuleSource: 'Global' or 'Survey-specific'.\n"
        "- RuleLabel: a short identifier or description of the rule.\n"
        "- RuleType: 'SKIP' or 'HARD'.\n"
        "- Variables: comma-separated variable names involved in the rule.\n"
        "- ApproxViolations: approximate number of violating rows in the RAW_DATA_SAMPLE_CSV.\n"
        "- ExampleInterviewIDs: a few example IDs from columns like interview__id, interview__key, or technicalid (if present).\n"
        "- Notes: brief, to-the-point comment on the nature of the violation.\n\n"
        "Output rules ONLY if you see clear violations in the sample.\n"
        "OUTPUT FORMAT REQUIREMENTS (VERY IMPORTANT):\n"
        "- If violations exist: output a SINGLE markdown table with header row and rows as described above.\n"
        "- Do NOT output any prose, bullets, or commentary before or after the table.\n"
        "- If NO violations are found: output EXACTLY this single line and nothing else:\n"
        "  No skip or hard-check violations detected in the sample."
    )

    return ask_ai(
        context,
        question,
        max_output_tokens=80000,
        usage_label="skip_logic_qc",
    )


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

    # ---------- NEW: summary of QC decisions ----------
    if "reject_decision" in df_int.columns:
        counts = df_int["reject_decision"].value_counts().to_dict()
        lines.append(f"QC decisions summary (Keep/Review/Reject counts): {counts}.")

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


def build_global_ai_context(
    raw_df: pd.DataFrame,
    data: dict,
    codebook_data: dict | None,
    max_rows: int = 200,
    max_codebook_rows: int = 200,
) -> str:
    """
    Build a combined context for the Global QC Q&A, including:
      - interview-level QC (df_int),
      - question-level QC (df_q),
      - issue table (raw_qc_issues),
      - a sample of raw data, and
      - codebook + logic sheets (if available).
    """
    parts: list[str] = []

    # --- Interview-level QC ---
    df_int = data.get("interview", pd.DataFrame())
    if isinstance(df_int, pd.DataFrame) and not df_int.empty:
        parts.append("=== INTERVIEW_QC_CSV ===")
        parts.append(df_int.head(max_rows).to_csv(index=False))

    # --- Question-level QC ---
    df_q = data.get("question", pd.DataFrame())
    if isinstance(df_q, pd.DataFrame) and not df_q.empty:
        parts.append("=== QUESTION_QC_CSV ===")
        parts.append(df_q.head(max_rows).to_csv(index=False))

    # --- Issue table (codebook + numeric/text) ---
    df_issues = data.get("raw_qc_issues", pd.DataFrame())
    if isinstance(df_issues, pd.DataFrame) and not df_issues.empty:
        parts.append("=== ISSUES_CSV ===")
        parts.append(df_issues.head(max_rows).to_csv(index=False))

    # --- Codebook and logic sheets ---
    if codebook_data:
        cb_df = codebook_data.get("codebook", pd.DataFrame())
        if isinstance(cb_df, pd.DataFrame) and not cb_df.empty:
            parts.append("=== CODEBOOK_CSV ===")
            parts.append(cb_df.head(max_codebook_rows).to_csv(index=False))

        gl_df = codebook_data.get("global_logic", pd.DataFrame())
        if isinstance(gl_df, pd.DataFrame) and not gl_df.empty:
            parts.append("=== GLOBAL_LOGIC_CSV ===")
            parts.append(gl_df.head(max_codebook_rows).to_csv(index=False))

        sv_df = codebook_data.get("survey_logic", pd.DataFrame())
        if isinstance(sv_df, pd.DataFrame) and not sv_df.empty:
            parts.append("=== SURVEY_LOGIC_CSV ===")
            parts.append(sv_df.head(max_codebook_rows).to_csv(index=False))

    # --- Raw data sample (all columns, limited rows) ---
    if isinstance(raw_df, pd.DataFrame) and not raw_df.empty:
        parts.append("=== RAW_DATA_SAMPLE_CSV ===")
        parts.append(raw_df.head(max_rows).to_csv(index=False))

    return "\n".join(parts)



def ai_check_numeric_string_consistency(
    df_raw: pd.DataFrame,
    max_rows: int = 500,
) -> str:
    """
    Use AI to check consistency between numeric fields and their 'spelled-out'
    text versions such as d2 vs d2x, n3 vs n3x, and key N-section variables
    (n2a/n2as, n2i/n2is, n7a/n7as) when available.

    Output format:
      - If mismatches are found: a SINGLE markdown table, one row per mismatch.
      - If no mismatches are found: the exact line
            No numeric/text mismatches detected in the sample.
    """
    if AI_CLIENT is None:
        return "AI is not configured. Please enter an API key in the sidebar and retry."

    # Candidate numeric/string pairs – adjust if you add more
    candidate_pairs = [
        ("d2", "d2x"),
        ("n3", "n3x"),
        ("n2a", "n2as"),
        ("n2i", "n2is"),
        ("n7a", "n7as"),
    ]
    available_pairs = [
        (a, b)
        for (a, b) in candidate_pairs
        if a in df_raw.columns and b in df_raw.columns
    ]
    if not available_pairs:
        return (
            "No numeric/string pairs (d2/d2x, n2*/n2*string, n3/n3x, n7a/n7as) "
            "found in this raw dataset."
        )

    # Identify ID columns we can show in the table
    id_cols = [
        c for c in ["interview__key", "interview__id", "technicalid"] if c in df_raw.columns
    ]
    all_cols: list[str] = list(
        dict.fromkeys(id_cols + [c for pair in available_pairs for c in pair])
    )

    df_subset = df_raw[all_cols].copy()

    # Limit rows for safety, but keep enough to be useful
    if len(df_subset) > max_rows:
        df_subset = df_subset.head(max_rows)

    sample_csv = df_subset.to_csv(index=False)

    context = (
        "You are checking consistency between numeric fields and their 'spelled-out' text versions "
        "in a World Bank Enterprise Survey raw dataset.\n\n"
        "You receive a single CSV table where each row is one interview. Columns include:\n"
        f"- ID columns (if present): {', '.join(id_cols) if id_cols else 'none'}\n"
        f"- Numeric/text pairs to check: {available_pairs}\n\n"
        "Here is the CSV:\n"
        f"{sample_csv}"
    )

    question = (
        "Using the CSV table, scan row by row for inconsistencies between each numeric field and its "
        "corresponding text field. Treat the text field as a human-readable representation of the numeric "
        "value (possibly with thousand separators or spaces). For each numeric/text pair:\n"
        "- Parse the text field into a numeric value where possible.\n"
        "- Consider values to match if they are equal up to small rounding differences; otherwise treat them as mismatched.\n\n"
        "Report ONLY mismatches. For each mismatch, output one row in a markdown table with the following columns:\n"
        "| Var | InterviewID | NumericValue | TextValue | ParsedFromText | Notes |\n\n"
        "Where:\n"
        "- Var: the numeric variable name (e.g. d2, n2a, n3).\n"
        "- InterviewID: use interview__id if present, otherwise interview__key, otherwise technicalid, otherwise leave blank.\n"
        "- NumericValue: value from the numeric column.\n"
        "- TextValue: raw string from the text column (e.g. d2x, n2as).\n"
        "- ParsedFromText: the numeric value you infer from TextValue (or blank if you truly cannot parse it).\n"
        "- Notes: brief, to-the-point comment on the nature of the mismatch (e.g. d2 much larger than parsed d2x).\n\n"
        "OUTPUT FORMAT REQUIREMENTS (VERY IMPORTANT):\n"
        "- If mismatches exist: output a SINGLE markdown table with the header row exactly as:\n"
        "    | Var | InterviewID | NumericValue | TextValue | ParsedFromText | Notes |\n"
        "  and one row per mismatch.\n"
        "- Do NOT output any prose, bullets, or commentary before or after the table.\n"
        "- If NO mismatches are found: output EXACTLY this single line and nothing else:\n"
        "  No numeric/text mismatches detected in the sample."
    )

    return ask_ai(
        context,
        question,
        max_output_tokens=80000,
        usage_label="numeric_string_consistency",
    )

def ai_check_string_qc(
    raw_df: pd.DataFrame,
    codebook_data: dict | None,
    max_rows: int = 200,
    max_vars: int = 60,
) -> str:
    """
    AI QC for string variables (excluding d2x, n2x).
    Focus: missing/invalid units and vague 'other specify' strings.

    Output:
      - If issues exist: SINGLE markdown table.
      - If no issues: EXACT line
            No string issues detected in the sample.
    """
    if AI_CLIENT is None:
        return "AI is not configured. Please enter an API key in the sidebar and retry."

    # 1) Identify string columns in raw_df
    string_cols = []
    for col in raw_df.columns:
        if pd.api.types.is_string_dtype(raw_df[col]):
            string_cols.append(col)

    # Exclude numeric-text columns we already treat specially
    exclude = {"d2x", "n2x"}
    string_cols = [c for c in string_cols if c not in exclude]

    if not string_cols:
        return "No string variables (other than d2x/n2x) were found in the raw dataset."

    # 2) Limit number of string variables for context, but try to keep it broad
    string_cols = string_cols[:max_vars]

    # 3) Optional: codebook snippet for these string variables
    cb_df = (
        codebook_data.get("codebook")
        if codebook_data and "codebook" in codebook_data
        else pd.DataFrame()
    )
    cb_subset_csv = ""
    if not cb_df.empty and "var" in cb_df.columns:
        cb_subset = cb_df[cb_df["var"].astype(str).isin(string_cols)].copy()
        if not cb_subset.empty:
            # Keep only the most useful columns for context
            keep_cols = [
                "var",
                "Description \n(see questionnaire for exact wording)",
                "section",
                "Var type",
                "Var range",
            ]
            keep_cols = [c for c in keep_cols if c in cb_subset.columns]
            cb_subset_csv = cb_subset[keep_cols].to_csv(index=False)

    # 4) Build raw-data sample for these string vars
    id_cols = [c for c in ["interview__key", "interview__id", "technicalid"] if c in raw_df.columns]
    cols_for_sample = list(dict.fromkeys(id_cols + string_cols))
    sample = raw_df[cols_for_sample].head(max_rows)
    sample_csv = sample.to_csv(index=False)

    context_parts = []

    if cb_subset_csv:
        context_parts.append("=== STRING_VARIABLES_CODEBOOK_CSV ===")
        context_parts.append(cb_subset_csv)

    context_parts.append("=== STRING_VARIABLES_SAMPLE_CSV ===")
    context_parts.append(sample_csv)

    context = "\n".join(context_parts)

    question = (
        "You are performing AI-based QC on string variables from a World Bank Enterprise Survey dataset.\n\n"
        "Inputs:\n"
        "- STRING_VARIABLES_CODEBOOK_CSV (if present): describes each string variable.\n"
        "- STRING_VARIABLES_SAMPLE_CSV: first {nrows} rows of raw data, containing only ID columns and string variables.\n\n"
        "Goals:\n"
        "- Scan ALL string columns in STRING_VARIABLES_SAMPLE_CSV VERY carefully (excluding d2x and n2x).\n"
        "- Focus especially on fields that should contain units or 'other specify' text, such as a3x, k342x, k392x, c34bx.\n"
        "- For each string variable, identify rows that show clear QC problems, for example:\n"
        "  • Missing or blank when a meaningful entry is expected.\n"
        "  • Vague or meaningless text (e.g. 'n/a', 'nothing', 'asd').\n"
        "  • Invalid or suspicious units (e.g. 'yes', 'no' in a unit field, units that don't make sense).\n"
        "  • Obvious miss-entries (e.g. numbers where text should be, or vice versa) based on the codebook description.\n\n"
        "OUTPUT FORMAT REQUIREMENTS (VERY IMPORTANT):\n"
        "- Report ONLY rows/variables where you see a clear string QC issue.\n"
        "- Output a SINGLE markdown table with the header row exactly as:\n"
        "    | Var | InterviewID | ExampleValue | IssueType | Notes |\n"
        "  and one row per issue type / example (you can group similar issues for the same variable).\n"
        "- Var: the string variable name (e.g. a3x, k342x, k392x, c34bx).\n"
        "- InterviewID: use interview__id if present, otherwise interview__key, otherwise technicalid, otherwise leave blank.\n"
        "- ExampleValue: a representative problematic value from the sample.\n"
        "- IssueType: a short label like 'Missing/blank', 'Vague text', 'Invalid unit', 'Suspicious value'.\n"
        "- Notes: brief and to-the-point explanation (max ~15 words).\n\n"
        "- Do NOT output any prose before or after the table.\n"
        "- If NO string issues are detected in the sample, output EXACTLY this single line and nothing else:\n"
        "  No string issues detected in the sample."
    ).format(nrows=len(sample))

    return ask_ai(
        context,
        question,
        max_output_tokens=80000,
        usage_label="string_qc",
    )

def ai_check_isic_consistency(
    raw_df: pd.DataFrame,
    raw_df_labeled: pd.DataFrame | None = None,
    max_rows: int = 200,
) -> str:
    """
    AI QC: Check consistency between d1a1a, d1a1x (description), and d1a2_v4 (vendor ISIC).

    IMPORTANT:
      - ALWAYS returns a markdown table with columns:
        | InterviewID | d1a1a | d1a1x | d1a2_v4_vendor | AI_ISIC | IssueType | Notes |
      - IssueType:
          OK                   -> consistent
          Vendor_ISIC_mismatch -> AI_ISIC != vendor code
          Unknown_description  -> description too vague to classify
          Internal_code_mismatch -> d1a1a sector conflicts with vendor/AI_ISIC
    """
    if AI_CLIENT is None:
        return "AI is not configured. Please enter an API key in the sidebar and retry."

    # Make sure required columns exist in numeric raw_df
    needed = ["d1a1a", "d1a1x", "d1a2_v4"]
    missing = [c for c in needed if c not in raw_df.columns]
    if missing:
        return f"Missing required columns for ISIC QC: {', '.join(missing)}"

    # ID columns to help identify interviews
    id_cols = [c for c in ["interview__id", "interview__key", "technicalid"] if c in raw_df.columns]

    cols = id_cols + ["d1a1a", "d1a1x", "d1a2_v4"]
    cols = list(dict.fromkeys(cols))  # dedupe while preserving order

    # Base sample from numeric raw_df (for IDs and numeric ISIC)
    sample = raw_df[cols].head(max_rows).copy()

    # Overwrite d1a1a with value labels when available
    if (
        raw_df_labeled is not None
        and not raw_df_labeled.empty
        and "d1a1a" in raw_df_labeled.columns
    ):
        sample["d1a1a"] = raw_df_labeled.loc[sample.index, "d1a1a"].astype(str)

    sample_csv = sample.to_csv(index=False)

    context = (
        "You are checking the consistency of main activity coding for an enterprise survey.\n\n"
        "For each row (interview), you are given:\n"
        "- d1a1x: free-text description of the establishment's main activity (what it does).\n"
        "- d1a2_v4: 4-digit ISIC Rev.4 code assigned by the vendor.\n"
        "- d1a1a: internal survey code for the main activity, shown using VALUE LABELS "
        "(e.g. 'Manufacturing of textiles', 'Retail trade') instead of numeric codes.\n"
        "- One or more interview identifiers (e.g. interview__id, interview__key, technicalid).\n\n"
        "You must use the description d1a1x to infer the best 4-digit ISIC Rev.4 code(s) and compare "
        "this with d1a2_v4 and with d1a1a.\n\n"
        "Here is the CSV with one row per interview:\n"
        "=== D1_ISIC_SAMPLE_CSV ===\n"
        f"{sample_csv}"
    )

    question = (
        "For EACH row in D1_ISIC_SAMPLE_CSV:\n"
        "1. Read d1a1x (description) and infer the best-matching 4-digit ISIC Rev.4 code(s) using UN ISIC Rev.4.\n"
        "   - Set AI_ISIC as:\n"
        "       CODE [PROB%]\n"
        "     or, if two plausible codes:\n"
        "       CODE1 [PROB1%] ; CODE2 [PROB2%]\n"
        "   - If description is too vague, set AI_ISIC = 'Unknown'.\n\n"
        "2. Compare AI_ISIC to the vendor code d1a2_v4:\n"
        "   - If the top AI code matches d1a2_v4 and the description fits, set IssueType = 'OK'.\n"
        "   - If AI_ISIC clearly points to a different ISIC from d1a2_v4, set IssueType = 'Vendor_ISIC_mismatch'.\n"
        "   - If description is too vague for a meaningful 4-digit classification, set IssueType = 'Unknown_description'.\n"
        "3. Consider d1a1a (VALUE LABEL) as a secondary check:\n"
        "   - If both AI_ISIC and vendor ISIC clearly belong to a different sector than d1a1a suggests, "
        "set IssueType = 'Internal_code_mismatch' (even if vendor and AI_ISIC agree).\n\n"
        "4. Notes: a SHORT explanation (< 20 words) explaining your reasoning for IssueType.\n\n"
        "InterviewID RULE:\n"
        "- Use interview__id if present; otherwise interview__key; otherwise technicalid; otherwise leave blank.\n\n"
        "OUTPUT FORMAT (VERY IMPORTANT):\n"
        "- Output ONLY a SINGLE markdown table with header:\n"
        "    | InterviewID | d1a1a | d1a1x | d1a2_v4_vendor | AI_ISIC | IssueType | Notes |\n"
        "  and one row per CSV row.\n"
        "- Do NOT output any prose before or after the table.\n"
        "- Do NOT use any other columns or headers."
    )

    return ask_ai(
        context,
        question,
        max_output_tokens=80000,
        usage_label="isic_consistency_qc",
    )


# ---------- Streamlit UI ----------

st.set_page_config(
    page_title="WBES Data QC Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("WBES Data QC Dashboard")
st.caption("Interactive QC triage + AI Co-Pilot for Enterprise Surveys")



with st.sidebar:
    st.header("Data & Filters")

    # --------- Data source choice ---------
    data_source = st.radio(
        "How do you want to load survey data?",
        ["Local Stata file (.dta)", "Survey Solutions API"],
        index=0,
        key="data_source_choice",
    )

    # --------- Common: Global Codebook (still required) ---------
    codebook_uploaded = st.file_uploader(
        "Upload Global Codebook (.xlsx)",
        type=["xlsx"],
        key="codebook_uploader",
        help="Global Codebook with 'codebook' and logic-check sheets.",
    )

    if codebook_uploaded is None:
        st.info("Upload the Global Codebook (.xlsx) to proceed.")
        st.stop()

    # Load codebook (cached)
    if "codebook_data" not in st.session_state or st.session_state.get("codebook_name") != codebook_uploaded.name:
        try:
            st.session_state["codebook_data"] = load_codebook_excel(codebook_uploaded.read())
            st.session_state["codebook_name"] = codebook_uploaded.name
        except Exception as e:
            st.error(f"Could not load codebook: {e}")
            st.stop()

    codebook_data = st.session_state["codebook_data"]
    st.success("Loaded Global Codebook.")

    # --------- Branch 1: Local Stata file ---------
    if data_source == "Local Stata file (.dta)":
        raw_uploaded = st.file_uploader(
            "Upload 'modified for QC data' File (.dta)",
            type=["dta", "DTA"],
            key="raw_dta_uploader",
            help="Raw Survey Solutions export in Stata format.",
        )

        if raw_uploaded is None:
            st.info("Upload the raw Stata file (.dta) to proceed.")
            st.stop()

        # Load raw data (cached)
        if "raw_df" not in st.session_state or st.session_state.get("raw_df_name") != raw_uploaded.name:
            raw_bytes = raw_uploaded.read()
            st.session_state["raw_bytes"] = raw_bytes
            st.session_state["raw_df"] = load_raw_stata(raw_bytes)
            st.session_state["raw_df_labeled"] = load_raw_stata_labeled(raw_bytes)
            st.session_state["raw_df_name"] = raw_uploaded.name

        raw_df = st.session_state["raw_df"]
        raw_df_labeled = st.session_state["raw_df_labeled"]

        st.success(f"Loaded raw data from file: {raw_df.shape[0]} interviews, {raw_df.shape[1]} variables.")

        # No SuSo client in this branch
        st.session_state["suso_client"] = None

    # --------- Branch 2: Survey Solutions API (SSAW) ---------
    else:
        st.markdown("**Survey Solutions API (SSAW)**")

        suso_url = st.text_input(
            "Survey Solutions HQ URL",
            value=st.session_state.get("suso_url", ""),
            help="e.g. https://yourserver.mysurvey.solutions",
        )

        auth_mode = st.radio(
            "Authentication",
            ["Token", "User/password"],
            key="suso_auth_mode",
            help="Token recommended. See Survey Solutions docs on token-based authentication.",
        )

        suso_client = None
        token = api_user = api_password = None

        if auth_mode == "Token":
            token = st.text_input(
                "API token",
                type="password",
                value=st.session_state.get("suso_token", ""),
            )
        else:
            api_user = st.text_input(
                "API user",
                value=st.session_state.get("suso_user", ""),
            )
            api_password = st.text_input(
                "API password",
                type="password",
                value=st.session_state.get("suso_password", ""),
            )

        workspace = st.text_input(
            "Workspace name",
            value=st.session_state.get("suso_workspace", "primary"),
            help="Default is 'primary'.",
        )

        # Try to build client when we have minimum info
        if suso_url and ((auth_mode == "Token" and token) or (auth_mode == "User/password" and api_user and api_password)):
            try:
                if auth_mode == "Token":
                    suso_client = ssaw.Client(suso_url, token=token, workspace=workspace)
                else:
                    suso_client = ssaw.Client(
                        suso_url,
                        api_user=api_user,
                        api_password=api_password,
                        workspace=workspace,
                    )
                st.session_state["suso_client"] = suso_client
                st.session_state["suso_url"] = suso_url
                st.session_state["suso_workspace"] = workspace
                if token:
                    st.session_state["suso_token"] = token
                if api_user:
                    st.session_state["suso_user"] = api_user
                if api_password:
                    st.session_state["suso_password"] = api_password

                st.success(f"Connected to Survey Solutions workspace '{workspace}'.")
            except Exception as e:
                st.error(f"Could not connect to Survey Solutions: {e}")
                suso_client = None

        else:
            suso_client = st.session_state.get("suso_client")

        # If we have a client, let user pick questionnaire + status and download
        raw_df = None
        raw_df_labeled = None

        if suso_client is not None:
            from ssaw import QuestionnairesApi

            try:
                q_api = QuestionnairesApi(suso_client)
                q_list = list(q_api.get_list())
            except Exception as e:
                q_list = []
                st.error(f"Could not list questionnaires: {e}")

            if q_list:
                options = {}
                for q in q_list:
                    label = f"{q.variable} v{q.version} – {q.title}"
                    options[label] = (str(q.id), int(q.version))

                selected_q_label = st.selectbox(
                    "Questionnaire",
                    options=list(options.keys()),
                    key="suso_questionnaire_select",
                )
                q_id, q_version = options[selected_q_label]

                status_options = [
                    "All",
                    "SupervisorAssigned",
                    "InterviewerAssigned",
                    "RejectedBySupervisor",
                    "Completed",
                    "ApprovedBySupervisor",
                    "RejectedByHeadquarters",
                    "ApprovedByHeadquarters",
                ]
                selected_status = st.selectbox(
                    "Interview status for export",
                    options=status_options,
                    index=status_options.index("Completed"),
                    key="suso_interview_status",
                )

                                # Ask for export zip password (optional)
                export_pwd = st.text_input(
                    "Export ZIP password (if set in HQ)",
                    type="password",
                    key="suso_export_password",
                    help="Leave blank if the export is not password-protected.",
                )

                save_dir_input = st.text_input(
                    "Optional folder to save SuSo export (.zip + main .dta)",
                    value=st.session_state.get("suso_save_dir", ""),
                    help=(
                        "Example: C:\\Users\\wb555954\\OneDrive - WBG\\Desktop\\WBES\\12_QC\\exports\n"
                        "Leave blank if you don't want to store a copy on disk."
                    ),
                    key="suso_save_dir_input",
                )

                if save_dir_input:
                    st.session_state["suso_save_dir"] = save_dir_input
                    

                

                if st.button("Download from SuSo and build QC", key="btn_suso_download"):
                    with st.spinner("Generating and downloading STATA export from Survey Solutions..."):
                        try:
                            # Use our custom export helper; we do NOT use ExportApi.get anymore.
                            raw_df, raw_df_labeled = suso_export_to_stata(
                                server=suso_url,
                                workspace=workspace,
                                quest_id=q_id,
                                version=q_version,
                                work_status=selected_status,
                                api_user=api_user if auth_mode == "User/password" else None,
                                api_password=api_password if auth_mode == "User/password" else None,
                                token=token if auth_mode == "Token" else None,
                                export_password=export_pwd or None,
                                save_dir=save_dir_input or None,   # <--- pass the optional folder
                            )

                            st.session_state["raw_df"] = raw_df
                            st.session_state["raw_df_labeled"] = raw_df_labeled
                            st.session_state["raw_df_name"] = f"suso_{q_id}${q_version}_{selected_status}.dta"
                            st.success(
                                f"Downloaded {raw_df.shape[0]} interviews, {raw_df.shape[1]} variables from SuSo."
                            )

                            if save_dir_input:                           # <-- THIS is the line you asked about
                                st.success(f"Export was also saved to: {save_dir_input}")
                        except Exception as e:
                            st.error(f"Failed to download/export data from SuSo: {e}")

                # --- NEW: let user choose which .dta file from the last SuSo export to use ---
        from pathlib import Path  # already imported at top, harmless to repeat here

        dta_paths = st.session_state.get("suso_dta_files")
        if dta_paths:
            st.markdown("**Choose which .dta file from the last SuSo export to use for QC**")

            labels = [Path(p).name for p in dta_paths]

            # Remember last choice if possible
            default_label = st.session_state.get("suso_selected_dta", labels[0])
            if default_label not in labels:
                default_label = labels[0]

            selected_label = st.selectbox(
                "Available .dta files in the export",
                options=labels,
                index=labels.index(default_label),
                key="suso_dta_file_select",
            )
            st.session_state["suso_selected_dta"] = selected_label

            if st.button("Load selected .dta for QC", key="btn_load_suso_dta"):
                selected_path = Path(dta_paths[labels.index(selected_label)])
                file_bytes = selected_path.read_bytes()

                # Reuse your cached loaders
                st.session_state["raw_df"] = load_raw_stata(file_bytes)
                st.session_state["raw_df_labeled"] = load_raw_stata_labeled(file_bytes)
                st.session_state["raw_df_name"] = selected_label

                st.success(
                    f"Loaded {st.session_state['raw_df'].shape[0]} interviews, "
                    f"{st.session_state['raw_df'].shape[1]} variables from {selected_label}."
                )
                    


        # After download, recover from session_state
        if raw_df is None:
            raw_df = st.session_state.get("raw_df")
            raw_df_labeled = st.session_state.get("raw_df_labeled")

        if raw_df is None or raw_df_labeled is None:
            st.info("Configure SuSo connection and click 'Download from SuSo and build QC'.")
            st.stop()

    # At this point, regardless of branch, we must have raw_df + codebook_data
    st.success(f"Loaded raw data: {raw_df.shape[0]} interviews, {raw_df.shape[1]} variables.")

    # Build QC structures directly from raw + codebook
    data = build_qc_data_from_raw(raw_df, codebook_data)
    df_int = data["interview"]
    df_q = data["question"]

    # (you can keep your DEBUG prints if you like)
    # st.write("DEBUG: raw_qc_issues shape", data["raw_qc_issues"].shape)
    # st.write(data["raw_qc_issues"].head())

    # Build / cache full QC context for AI
    st.session_state["full_qc_context"] = build_global_ai_context(
        raw_df,
        data,
        codebook_data,
    )

     # -------- OpenAI API key --------
    st.markdown("---")
    st.subheader("OpenAI API key")

    default_key = st.session_state.get("api_key", "")
    api_key_input = st.text_input(
        "Enter OpenAI API key",
        type="password",
        value=default_key,
        help="Key is kept only in this session; do not share this app publicly with a real key.",
        key="openai_api_key_input",
    )

    if api_key_input:
        st.session_state["api_key"] = api_key_input

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
        interviewer_filter = st.multiselect(
            "Filter by interviewer code",
            options=interviewer_vals,
            default=[],
            key="interviewer_filter_sidebar",
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

    # Store in session_state so we can read them after the sidebar block
    st.session_state["selected_idu"] = selected_idu
    st.session_state["only_flagged_interviews"] = only_flagged_interviews
    st.session_state["priority_filter"] = priority_filter
    st.session_state["interviewer_filter"] = interviewer_filter
    st.session_state["min_inf_threshold"] = min_inf_threshold


    # OpenAI key and filters (unchanged)...
    # (keep your existing OpenAI API key block, interview filters, question QC flags, etc.)


# Recompute dynamic question flags based on sidebar threshold
# Recover sidebar filter values from session_state (with sane defaults)
selected_idu = st.session_state.get("selected_idu", "All")
only_flagged_interviews = st.session_state.get("only_flagged_interviews", True)
priority_filter = st.session_state.get("priority_filter", ["High", "Medium", "Low"])
interviewer_filter = st.session_state.get("interviewer_filter", [])
min_inf_threshold = st.session_state.get("min_inf_threshold", 70)

# Recompute dynamic question flags based on sidebar threshold

df_q_dyn = df_q.copy()
df_q_dyn["qc_flag"] = (
    (df_q_dyn["response_rate_informative_num"] < min_inf_threshold)
    | df_q_dyn["SKIPS_by_question"].notna()
    | df_q_dyn["INVALIDS_by_question"].notna()
)


# ---------- Tabs ----------
tab_dash, tab_int, tab_qtab, tab_details, tab_checks, tab_ai, tab_report = st.tabs(
    [
        "Dashboard",
        "Interviews",
        "Questions",
        "Issue details",
        "Check dictionary",
        "AI Data QC",
        "Report (TTL + Vendor)",
    ]
)

with tab_dash:
    st.subheader("Overall QC health (AI-enhanced)")

    ai_df_int = st.session_state.get("ai_df_int")
    ai_stats = st.session_state.get("ai_qc_stats")

    if isinstance(ai_df_int, pd.DataFrame) and not ai_df_int.empty and ai_stats:
        base_int = ai_df_int
        use_ai = True
    else:
        base_int = df_int
        use_ai = False

    # ----- Top metrics -----
    n_interviews = len(base_int)

    avg_asked = base_int.get("share_properly_asked_answered_num")
    avg_asked = float(avg_asked.mean()) if avg_asked is not None else np.nan

    avg_inf = base_int.get("share_proper_informative_num")
    avg_inf = float(avg_inf.mean()) if avg_inf is not None else np.nan

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("# interviews", n_interviews)
    c2.metric(
        "Avg. % properly asked",
        f"{avg_asked:.1f}%" if not np.isnan(avg_asked) else "n/a",
    )
    c3.metric(
        "Avg. % informative answers",
        f"{avg_inf:.1f}%" if not np.isnan(avg_inf) else "n/a",
    )

    if use_ai and ai_stats:
        c4.metric(
            "AI-flagged interviews",
            f"{ai_stats['n_ai_issues_any']} (issues in ≥1 AI check)",
        )
    else:
        if "any_issue" in base_int.columns:
            c4.metric("# interviews with rule-based issues", int(base_int["any_issue"].sum()))
        else:
            c4.metric("# interviews with issues", "n/a")

    # ----- AI-enhanced QC summary -----
    if use_ai and ai_stats:
        st.markdown("---")
        st.subheader("AI-enhanced QC summary")

        c5, c6, c7, c8 = st.columns(4)
        c5.metric(
            "AI QC score (avg)",
            f"{ai_stats['avg_ai_qc_score']:.1f}" if ai_stats["avg_ai_qc_score"] is not None else "n/a",
        )
        c6.metric(
            "AI Reject",
            ai_stats["n_ai_reject"],
            help="Interviews where AI recommends Reject based on ISIC, skip/hard checks or many numeric mismatches.",
        )
        c7.metric(
            "AI Review",
            ai_stats["n_ai_review"],
            help="Interviews with some AI issues but not hard reject.",
        )
        c8.metric(
            "Total AI issues",
            sum(ai_stats["ai_issue_summary"].values()),
            help=str(ai_stats["ai_issue_summary"]),
        )

        # QC tiers by AI tier
        tier_col = "ai_qc_tier" if "ai_qc_tier" in base_int.columns else "qc_tier"
        if tier_col in base_int.columns:
            c9, c10 = st.columns(2)
            with c9:
                st.markdown("**QC tiers (AI)**" if tier_col == "ai_qc_tier" else "**QC tiers**")
                tier_counts = base_int[tier_col].value_counts().sort_index()
                st.bar_chart(tier_counts)

            with c10:
                st.markdown("**AI issue counts by type**")
                issue_summary = ai_stats["ai_issue_summary"]
                issue_df = (
                    pd.DataFrame(
                        {"issue_type": list(issue_summary.keys()), "count": list(issue_summary.values())}
                    )
                    .set_index("issue_type")
                )
                st.bar_chart(issue_df)

        # Worst interviews by AI QC score
        if "ai_qc_score" in base_int.columns:
            st.markdown("---")
            st.markdown("**Worst interviews by AI QC score (top 15)**")
            worst = base_int.sort_values("ai_qc_score").head(15)
            cols_show = [
                "idu",
                "technicalid",
                "ai_qc_score",
                "ai_qc_tier",
                "ai_issue_total",
                "ai_numeric_issues",
                "ai_string_issues",
                "ai_isic_issues",
                "ai_skip_issues",
                "ai_innov_conflicts",
                "ai_reject_decision",
                "ai_reject_reasons",
            ]
            cols_show = [c for c in cols_show if c in worst.columns]
            st.dataframe(worst[cols_show], use_container_width=True)

    # ----- Existing “status by interviewer” block (but prefer AI scores if available) -----
    st.markdown("---")
    st.subheader("Status by interviewer (a12)")

    if INTERVIEWER_VAR in base_int.columns:
        if interviewer_filter:
            df_int_for_intv = base_int[base_int[INTERVIEWER_VAR].astype(str).isin(interviewer_filter)]
        else:
            df_int_for_intv = base_int

        interviewer_summary = build_interviewer_summary(
            df_int_for_intv,
            interviewer_var=INTERVIEWER_VAR,
        )

        if interviewer_summary.empty:
            st.info("No interviewer information (a12) available (or no data after filters).")
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
                    key="top_n_interviewers_dash",
                )
            with c_sort:
                sort_option = st.selectbox(
                    "Sort interviewers by",
                    [
                        "Worst average QC score (AI if available)",
                        "Highest % interviews with issues",
                        "Most interviews completed",
                    ],
                    key="sort_interviewers_by_dash",
                )

            interviewer_sorted = interviewer_summary.copy()
            score_col = "ai_qc_score" if use_ai and "ai_qc_score" in df_int_for_intv.columns else "qc_score"

            if sort_option.startswith("Worst") and score_col in interviewer_sorted.columns:
                interviewer_sorted = interviewer_sorted.sort_values(score_col, ascending=True)
            elif sort_option.startswith("Highest %") and "pct_with_issues" in interviewer_sorted.columns:
                interviewer_sorted = interviewer_sorted.sort_values("pct_with_issues", ascending=False)
            elif "n_interviews" in interviewer_sorted.columns:
                interviewer_sorted = interviewer_sorted.sort_values("n_interviews", ascending=False)

            interviewer_top = interviewer_sorted.head(int(top_n))

            c_table, c_chart = st.columns([3, 2])
            with c_table:
                preferred_cols = [
                    "interviewer",
                    "n_interviews",
                    "n_with_issues",
                    "pct_with_issues",
                    score_col,
                    "avg_share_asked",
                    "avg_share_informative",
                ]
                preferred_cols = [c for c in preferred_cols if c in interviewer_top.columns]
                other_cols = [c for c in interviewer_top.columns if c not in preferred_cols]
                cols_for_display = preferred_cols + other_cols
                st.markdown("**Per-interviewer QC summary**")
                st.dataframe(interviewer_top[cols_for_display], use_container_width=True)

            with c_chart:
                if score_col in interviewer_top.columns:
                    st.markdown("**Average QC score by interviewer**")
                    chart_data = interviewer_top.set_index("interviewer")[[score_col]]
                    st.bar_chart(chart_data)
                else:
                    st.info("QC score not available for chart.")
    else:
        st.info("Column a12 (interviewer code) is not present in the QC data.")

with tab_int:
    st.subheader("Interview-level QC triage")

    # Prefer AI-enhanced QC if we have it; otherwise fall back to raw df_int
    ai_df_int = st.session_state.get("ai_df_int")
    if isinstance(ai_df_int, pd.DataFrame) and not ai_df_int.empty:
        base_int = ai_df_int
    else:
        base_int = df_int  # raw QC-only version

    view = base_int.copy()

    # Filter by interviewer (a12) if selected
    if INTERVIEWER_VAR in view.columns and interviewer_filter:
        view = view[view[INTERVIEWER_VAR].astype(str).isin(interviewer_filter)]

    # Only flagged interviews – use AI flag if available, else rule-based
    if only_flagged_interviews:
        flag_col = None
        if "ai_issue_total" in view.columns:
            flag_col = "ai_issue_total"
        elif "any_issue" in view.columns:
            flag_col = "any_issue"

        if flag_col is not None:
            if flag_col == "ai_issue_total":
                view = view[view["ai_issue_total"] > 0]
            else:
                view = view[view["any_issue"]]
        # if neither col exists, we skip this filter so you still see data

    # Priority filter (if available)
    if priority_filter and "priority" in view.columns:
        view = view[view["priority"].isin(priority_filter)]

    # Specific interview choice
    if selected_idu != "All":
        view = view[view["idu"].astype(str) == selected_idu]

    # Columns to show – prefer AI columns where they exist
    cols_to_show = [
        "idu",
        "technicalid",
        INTERVIEWER_VAR if INTERVIEWER_VAR in view.columns else None,
        "share_properly_asked_answered" if "share_properly_asked_answered" in view.columns else None,
        "share_proper_informative" if "share_proper_informative" in view.columns else None,
        "ai_qc_score" if "ai_qc_score" in view.columns else "qc_score",
        "ai_qc_tier" if "ai_qc_tier" in view.columns else "qc_tier",
        "priority" if "priority" in view.columns else None,
        "ai_reject_decision" if "ai_reject_decision" in view.columns else "reject_decision",
        "ai_reject_reasons" if "ai_reject_reasons" in view.columns else "reject_reasons",
        "ai_issue_total" if "ai_issue_total" in view.columns else None,
        "ai_numeric_issues" if "ai_numeric_issues" in view.columns else None,
        "ai_string_issues" if "ai_string_issues" in view.columns else None,
        "ai_skip_issues" if "ai_skip_issues" in view.columns else None,
        "ai_isic_issues" if "ai_isic_issues" in view.columns else None,
        "ai_innov_conflicts" if "ai_innov_conflicts" in view.columns else None,
        "OUTLIERS_by_interview" if "OUTLIERS_by_interview" in view.columns else None,
        "LOGIC_CHECKS_by_interview" if "LOGIC_CHECKS_by_interview" in view.columns else None,
        "PRODUCTIVITY_by_interview" if "PRODUCTIVITY_by_interview" in view.columns else None,
        "GPS_by_interview" if "GPS_by_interview" in view.columns else None,
        "STRINGS_by_interview" if "STRINGS_by_interview" in view.columns else None,
        "BR_OUTLIERS_by_interview" if "BR_OUTLIERS_by_interview" in view.columns else None,
        "REST_OUTLIERS_by_interview" if "REST_OUTLIERS_by_interview" in view.columns else None,
        "VENDOR_COMMENTS" if "VENDOR_COMMENTS" in view.columns else None,
    ]
    cols_to_show = [c for c in cols_to_show if c is not None and c in view.columns]

    if view.empty:
        st.info("No interviews match the current filters. Try turning off 'only flagged' or widening filters.")
    else:
        sort_score_col = "ai_qc_score" if "ai_qc_score" in view.columns else "qc_score"
        sort_cols = []
        asc = []
        if "priority_rank" in view.columns:
            sort_cols.append("priority_rank")
            asc.append(True)
        if sort_score_col in view.columns:
            sort_cols.append(sort_score_col)
            asc.append(True)

        if sort_cols:
            view_sorted = view.sort_values(sort_cols, ascending=asc)
        else:
            view_sorted = view

        st.dataframe(view_sorted[cols_to_show], use_container_width=True)

        csv = view_sorted[cols_to_show].to_csv(index=False).encode("utf-8")
        st.download_button(
            label="Download filtered interviews as CSV",
            data=csv,
            file_name="qc_interviews_filtered_ai.csv",
            mime="text/csv",
        )
with tab_qtab:
    st.subheader("Question-level QC")

    # Prefer AI-enriched question table if you've added it, otherwise raw df_q_dyn
    ai_df_q = st.session_state.get("ai_df_q")
    if isinstance(ai_df_q, pd.DataFrame) and not ai_df_q.empty:
        base_q = ai_df_q
    else:
        base_q = df_q_dyn

    if base_q is None or base_q.empty:
        st.info("No question-level QC data available. Check that raw data + codebook loaded correctly.")
    else:
        # Simple initial view: you can refine later
        st.dataframe(base_q, use_container_width=True)


# ---------- Issue details tab (new, codebook + numeric/text) ----------
with tab_details:
    st.subheader("Detailed issues from raw QC (codebook + numeric/text)")

    issues = data.get("raw_qc_issues", pd.DataFrame()).copy()

    if issues.empty:
        st.info(
            "No structured QC issues have been detected yet. "
            "These are generated automatically from raw data + Global Codebook."
        )
    else:
        if "idu" in issues.columns:
            issues["idu"] = issues["idu"].astype(str)

        # --- Filters ---
        c1, c2, c3, c4 = st.columns(4)

        with c1:
            idu_all = (
                sorted(issues["idu"].dropna().astype(str).unique().tolist())
                if "idu" in issues.columns
                else []
            )
            idu_filter = st.multiselect(
                "Filter by interview (idu)",
                options=idu_all,
                default=[selected_idu] if selected_idu != "All" and selected_idu in idu_all else [],
                key="issues_idu_filter",
            )

        with c2:
            vars_all = (
                sorted(issues["var"].astype(str).unique().tolist())
                if "var" in issues.columns
                else []
            )
            var_filter = st.multiselect(
                "Filter by variable",
                options=vars_all,
                default=[],
                key="issues_var_filter",
            )

        with c3:
            types_all = (
                sorted(issues["issue_type"].astype(str).unique().tolist())
                if "issue_type" in issues.columns
                else []
            )
            type_filter = st.multiselect(
                "Filter by issue type",
                options=types_all,
                default=types_all,  # show all by default
                key="issues_type_filter",
            )

        with c4:
            sources_all = (
                sorted(issues["source"].astype(str).unique().tolist())
                if "source" in issues.columns
                else []
            )
            source_filter = st.multiselect(
                "Filter by source",
                options=sources_all,
                default=sources_all,
                key="issues_source_filter",
            )

        # Apply filters
        view = issues.copy()
        if idu_filter and "idu" in view.columns:
            view = view[view["idu"].astype(str).isin(idu_filter)]
        if var_filter and "var" in view.columns:
            view = view[view["var"].astype(str).isin(var_filter)]
        if type_filter and "issue_type" in view.columns:
            view = view[view["issue_type"].astype(str).isin(type_filter)]
        if source_filter and "source" in view.columns:
            view = view[view["source"].astype(str).isin(source_filter)]

        # --- Overview ---
        st.markdown("---")
        st.markdown("### Issue overview")

        if not view.empty:
            c_top, c_bottom = st.columns([2, 1])

            with c_top:
                st.markdown("**Issues by variable (top 20)**")
                if "var" in view.columns:
                    var_counts = (
                        view["var"]
                        .astype(str)
                        .value_counts()
                        .head(20)
                        .rename_axis("var")
                        .reset_index(name="count")
                        .set_index("var")
                    )
                    st.bar_chart(var_counts)

            with c_bottom:
                st.markdown("**Issues by type/source**")

                if "issue_type" in view.columns:
                    if "source" in view.columns:
                        type_counts = (
                            view.groupby(["issue_type", "source"], dropna=False)["var"]
                            .count()
                            .reset_index()
                            .rename(columns={"var": "count"})
                        )
                    else:
                        # Fallback: group only by issue_type if source is missing
                        type_counts = (
                            view.groupby(["issue_type"], dropna=False)["var"]
                            .count()
                            .reset_index()
                            .rename(columns={"var": "count"})
                        )
                    st.dataframe(type_counts, use_container_width=True)
                else:
                    st.info("No 'issue_type' column in issues; cannot summarise by type/source.")


        # --- Detail table ---
        st.markdown("---")
        st.markdown("### Issue detail (filtered)")

        if view.empty:
            st.info("No issues to display for the current filters.")
        else:
            display_cols = []
            for c in [
                "idu",
                "technicalid",
                "var",
                "issue_type",
                "source",
                "detail",
                "interview_id",
                "interview_index",
            ]:
                if c in view.columns:
                    display_cols.append(c)

            st.dataframe(view[display_cols], use_container_width=True)

            csv_issues = view[display_cols].to_csv(index=False).encode("utf-8")
            st.download_button(
                "Download filtered issues as CSV",
                data=csv_issues,
                file_name="qc_issues_filtered.csv",
                mime="text/csv",
                key="download_qc_issues_filtered",
            )


# ---------- Check dictionary tab ----------
with tab_checks:
    st.subheader("QC check dictionary (from Global Codebook)")

    codebook_data = st.session_state.get("codebook_data")

    if not codebook_data:
        st.info("Upload the Global Codebook in the sidebar.")
    else:
        df_global = codebook_data.get("global_logic", pd.DataFrame())
        df_survey = codebook_data.get("survey_logic", pd.DataFrame())

        if (df_global is None or df_global.empty) and (df_survey is None or df_survey.empty):
            st.info("No logic-check sheets found in the codebook.")
        else:
            tab_g, tab_s = st.tabs(["Global Logic Checks", "Survey-specific Checks"])

            with tab_g:
                if df_global is None or df_global.empty:
                    st.info("No 'Global Logic Checks' sheet.")
                else:
                    search = st.text_input("Search global checks", key="search_global_checks")
                    view = df_global.copy()
                    if search:
                        view = view[
                            view.apply(
                                lambda row: row.astype(str).str.contains(
                                    search, case=False, na=False
                                ).any(),
                                axis=1,
                            )
                        ]
                    st.dataframe(view, use_container_width=True)

            with tab_s:
                if df_survey is None or df_survey.empty:
                    st.info("No 'Logic Checks for THIS surv.ONLY' sheet.")
                else:
                    search2 = st.text_input("Search survey-specific checks", key="search_survey_checks")
                    view2 = df_survey.copy()
                    if search2:
                        view2 = view2[
                            view2.apply(
                                lambda row: row.astype(str).str.contains(
                                    search2, case=False, na=False
                                ).any(),
                                axis=1,
                            )
                        ]
                    st.dataframe(view2, use_container_width=True)


# ---------- AI Co-Pilot tab ----------
with tab_ai:
    st.subheader("AI Co-Pilot for QC")

    if AI_CLIENT is None:
        st.info(ai_available_text())
    else:
        # Make sure full QC context is available once
                # Make sure full QC context is available once (raw + QC + codebook)
        full_ctx = st.session_state.get("full_qc_context")
        if not full_ctx:
            raw_df_tab = st.session_state.get("raw_df")
            codebook_tab = st.session_state.get("codebook_data")
            full_ctx = build_global_ai_context(raw_df_tab, data, codebook_tab)
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

        usage = st.session_state.get("ai_usage_total")
        if usage:
            with st.expander("AI token usage (session)", expanded=False):
                st.write(
                    f"Input: {usage['input']:,}, "
                    f"Reasoning: {usage['reasoning']:,}, "
                    f"Output: {usage['output']:,}"
                )
                log = st.session_state.get("ai_usage_log") or []
                if log:
                    st.dataframe(pd.DataFrame(log), use_container_width=True)


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
                max_output_tokens=60000,
                usage_label="global_qc",
            )

        if st.session_state["ai_global_answer"]:
            st.markdown("**AI answer (based on the full QC dump):**")
            st.markdown(st.session_state["ai_global_answer"])


                  # ----------------------------------------
        # 2. Per-interview AI summary (row-by-row)
        # ----------------------------------------
        st.markdown("---")
        st.markdown("### 2. Per-interview AI summary (row-by-row)")

        idu_options = sorted(df_int["idu"].astype(str).unique().tolist())

        st.session_state.setdefault("ai_single_summary", "")
        st.session_state.setdefault("ai_single_summary_table", pd.DataFrame())

        # --- Single interview summary ---
        idu_for_summary = st.selectbox(
            "Select interview (idu) for one-off AI summary",
            idu_options,
            key="idu_ai_summary",
        )

        if st.button("Generate AI summary for selected interview", key="btn_summary_one"):
            ctx_interview = build_interview_context(idu_for_summary, data)
            q = (
                "Summarise the data-quality issues for THIS SINGLE interview and list "
                "3–5 concrete follow-up actions for the vendor or enumerator. "
                "Do not discuss other interviews or global patterns."
            )
            st.session_state["ai_single_summary"] = ask_ai(
                ctx_interview,
                q,
                max_output_tokens=80000,
                usage_label=f"interview_{idu_for_summary}",
            )

        if st.session_state["ai_single_summary"]:
            st.markdown("**AI summary for this interview:**")
            st.markdown(st.session_state["ai_single_summary"])

        # --- Batch, row-by-row summaries into a table ---
        st.markdown("#### Batch interview summaries (one API call per interview)")

        idu_multi_single = st.multiselect(
            "Interviews to summarise (leave empty and tick 'Use all' to include everyone)",
            idu_options,
            key="idu_multi_single_summary",
        )

        use_all_single = st.checkbox(
            "Use all interviews for batch summaries",
            value=False,
            key="chk_single_summary_all_interviews_batch",
        )

        if st.button("Run batch per-interview summaries", key="btn_batch_single_summary"):
            if not idu_multi_single and not use_all_single:
                st.warning("Select at least one interview or tick 'Use all interviews'.")
            else:
                if use_all_single:
                    selected_ids = idu_options
                else:
                    selected_ids = [str(x) for x in idu_multi_single]

                results = []
                progress = st.progress(0.0)
                status = st.empty()
                table_placeholder = st.empty()

                total = len(selected_ids)
                processed = 0

                for i, idu_val in enumerate(selected_ids, start=1):
                    ctx_interview = build_interview_context(idu_val, data)
                    q = (
                        "Summarise the data-quality issues for THIS SINGLE interview and list "
                        "3–5 concrete follow-up actions for the vendor or enumerator. "
                        "Be concise (≤180 words)."
                    )
                    answer = ask_ai(
                        ctx_interview,
                        q,
                        max_output_tokens=80000,
                        usage_label=f"interview_batch_{idu_val}",
                    )

                    results.append({"idu": str(idu_val), "summary": answer})
                    processed += 1

                    df_res = pd.DataFrame(results)
                    st.session_state["ai_single_summary_table"] = df_res

                    progress.progress(i / total)
                    status.text(f"Processed {processed} / {total} interviews")
                    table_placeholder.dataframe(df_res, use_container_width=True)

        if not st.session_state["ai_single_summary_table"].empty:
            st.markdown("**Batch per-interview summaries (row-by-row):**")
            st.dataframe(st.session_state["ai_single_summary_table"], use_container_width=True)

            csv_batch = st.session_state["ai_single_summary_table"].to_csv(index=False).encode("utf-8")
            st.download_button(
                "Download summaries as CSV",
                data=csv_batch,
                file_name="qc_interview_summaries_ai.csv",
                mime="text/csv",
                key="dl_ai_single_summary_table",
            )




        st.markdown("---")

                # ----------------------------------------
        # 3. Overall QC status for selected / all interviews (AI table)
        # ----------------------------------------
        st.markdown("### 3. Overall QC status for selected / all interviews")

        idu_multi_summary = st.multiselect(
            "Select interviews (idu) to include (leave blank and tick 'Use all' to include all interviews)",
            sorted(df_int["idu"].astype(str).unique().tolist()),
            key="idu_multi_summary",
        )

        use_all_for_summary = st.checkbox(
            "Use all interviews",
            value=False,
            key="chk_summary_all_interviews",
        )

        if st.button("Generate AI QC status table", key="btn_overall_summary_table"):
            if not idu_multi_summary and not use_all_for_summary:
                st.warning("Select at least one interview or tick 'Use all interviews'.")
            else:
                if use_all_for_summary:
                    selected_ids = set(df_int["idu"].astype(str).tolist())
                else:
                    selected_ids = set(str(x) for x in idu_multi_summary)

                df_sel = df_int[df_int["idu"].astype(str).isin(selected_ids)].copy()

                # Keep a compact set of columns for AI
                cols_for_ai = []
                for c in [
                    "idu",
                    "technicalid",
                    INTERVIEWER_VAR if INTERVIEWER_VAR in df_sel.columns else None,
                    "qc_score",
                    "qc_tier",
                    "priority",
                    "raw_qc_issue_count" if "raw_qc_issue_count" in df_sel.columns else None,
                    "n_major_issues",
                    "reject_decision" if "reject_decision" in df_sel.columns else None,
                    "share_properly_asked_answered_num",
                    "share_proper_informative_num",
                ]:
                    if c is not None and c in df_sel.columns:
                        cols_for_ai.append(c)

                df_ai = df_sel[cols_for_ai].copy()
                csv_block = df_ai.to_csv(index=False)

                context_table = (
                    "SELECTED_INTERVIEWS_CSV:\n"
                    + csv_block
                )

                question = (
                    "You are given SELECTED_INTERVIEWS_CSV where each row is a single interview's QC summary "
                    "from a World Bank Enterprise Survey. Columns may include:\n"
                    "- idu\n"
                    "- technicalid\n"
                    "- interviewer code (e.g. a12)\n"
                    "- qc_score\n"
                    "- qc_tier\n"
                    "- priority\n"
                    "- raw_qc_issue_count\n"
                    "- n_major_issues\n"
                    "- reject_decision\n"
                    "- share_properly_asked_answered_num\n"
                    "- share_proper_informative_num\n\n"
                    "Summarise the QC status of these interviews in a SINGLE markdown table with the following columns:\n"
                    "| idu | technicalid | qc_score | qc_tier | priority | issues | reject_decision | key_issues |\n\n"
                    "Where:\n"
                    "- issues: a compact label summarising raw_qc_issue_count and n_major_issues "
                    "  (for example: '5 total / 3 major'). If those columns are missing, leave this cell blank.\n"
                    "- key_issues: a very short note (max ~15 words) highlighting the main QC concern for this interview, "
                    "  based ONLY on the columns available.\n\n"
                    "OUTPUT FORMAT REQUIREMENTS (VERY IMPORTANT):\n"
                    "- Output ONLY a markdown table with the header row exactly as:\n"
                    "    | idu | technicalid | qc_score | qc_tier | priority | issues | reject_decision | key_issues |\n"
                    "  and one row per interview in SELECTED_INTERVIEWS_CSV.\n"
                    "- If a column is missing in the CSV (e.g. technicalid or reject_decision), leave that cell blank.\n"
                    "- Do NOT output any prose before or after the table."
                )

                st.session_state["ai_overall_summary"] = ask_ai(
                    context_table,
                    question,
                    max_output_tokens=80000,
                    usage_label="overall_qc_table",
                )

        if st.session_state.get("ai_overall_summary"):
            st.markdown("**AI QC status table:**")
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

                answer = ask_ai(
                    combined_ctx,
                    q,
                    max_output_tokens=80000,
                    usage_label="vendor_email",
                )

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
                # ----------------------------------------
                # ----------------------------------------
                # ----------------------------------------
        # 5. AI numeric–string consistency (row-by-row, unified table)
        # ----------------------------------------
        st.markdown("---")
        st.markdown("### 5. AI numeric–string consistency (d2/d2x, n2*/n2*string, n3/n3x) – row-by-row")

        raw_df = st.session_state.get("raw_df")
        raw_df_labeled = st.session_state.get("raw_df_labeled")
        if raw_df is None:
            st.info("Upload raw Stata data (.dta) in the sidebar to enable this check.")
        else:
            st.session_state.setdefault("ai_numeric_string_results", pd.DataFrame())

            idu_options_num = sorted(df_int["idu"].astype(str).unique().tolist())
            idu_for_num = st.multiselect(
                "Interviews to include in the numeric–string check",
                idu_options_num,
                key="idu_numeric_string",
            )

            use_all_num = st.checkbox(
                "Use all interviews for numeric–string QC",
                value=False,
                key="chk_numeric_all",
            )

            if st.button("Run numeric–string check (row-by-row)", key="btn_ai_num_string_rowwise"):
                if not idu_for_num and not use_all_num:
                    st.warning("Select at least one interview or tick 'Use all interviews'.")
                else:
                    if use_all_num:
                        selected_ids = idu_options_num
                    else:
                        selected_ids = [str(x) for x in idu_for_num]

                    unified_rows = []
                    progress = st.progress(0.0)
                    status = st.empty()
                    table_placeholder = st.empty()

                    total = len(selected_ids)
                    processed = 0

                    for i, idu_val in enumerate(selected_ids, start=1):
                        row_df = get_raw_rows_for_idu(raw_df, idu_val)
                        if row_df.empty:
                            continue

                        report_text = ai_check_numeric_string_consistency(row_df)
                        processed += 1

                        idu_str = str(idu_val)
                        default_interview_id = get_primary_interview_id(row_df)

                        # Case 1: No mismatches
                                                # --- CASE 1: AI says there are no mismatches ---
                        if isinstance(report_text, str) and report_text.strip().startswith(
                            "No numeric/text mismatches detected"
                        ):
                            # For no-issue interviews, still show each numeric/string pair
                            candidate_pairs = [
                                ("d2", "d2x"),
                                ("n3", "n3x"),
                                ("n2a", "n2as"),
                                ("n2i", "n2is"),
                                ("n7a", "n7as"),
                            ]

                            for num_col, txt_col in candidate_pairs:
                                if num_col not in row_df.columns or txt_col not in row_df.columns:
                                    continue

                                num_val = row_df[num_col].iloc[0]
                                txt_val = row_df[txt_col].iloc[0]

                                # parse text into numeric if possible
                                txt_str = "" if pd.isna(txt_val) else str(txt_val)
                                cleaned = txt_str.replace(",", "").strip()
                                try:
                                    parsed_val = pd.to_numeric(cleaned, errors="coerce")
                                except Exception:
                                    parsed_val = np.nan

                                unified_rows.append(
                                    {
                                        "idu": idu_str,
                                        "Var": num_col,
                                        "InterviewID": default_interview_id or idu_str,
                                        "NumericValue": "" if pd.isna(num_val) else str(num_val),
                                        "TextValue": txt_str,
                                        "ParsedFromText": "" if pd.isna(parsed_val) else str(parsed_val),
                                        "Notes": "No numeric/text mismatches detected",
                                    }
                                )

                        # --- CASE 2: AI returned a markdown table with mismatches ---
                        else:
                            parsed = parse_markdown_table(
                                report_text,
                                expected_first_headers=["Var", "InterviewID"],
                            )
                            if not parsed:
                                # fallback: just dump the text in Notes
                                unified_rows.append(
                                    {
                                        "idu": idu_str,
                                        "Var": "",
                                        "InterviewID": default_interview_id or idu_str,
                                        "NumericValue": "",
                                        "TextValue": "",
                                        "ParsedFromText": "",
                                        "Notes": report_text.strip(),
                                    }
                                )
                            else:
                                for row in parsed:
                                    unified_rows.append(
                                        {
                                            "idu": idu_str,
                                            "Var": row.get("Var", ""),
                                            "InterviewID": row.get("InterviewID", default_interview_id or idu_str),
                                            "NumericValue": row.get("NumericValue", ""),
                                            "TextValue": row.get("TextValue", ""),
                                            "ParsedFromText": row.get("ParsedFromText", ""),
                                            "Notes": row.get("Notes", ""),
                                        }
                                    )


                        df_res = pd.DataFrame(unified_rows)
                        st.session_state["ai_numeric_string_results"] = df_res

                        progress.progress(i / total)
                        status.text(f"Processed {processed} / {total} interviews")

                        if not df_res.empty:
                            styled = df_res.style.apply(highlight_issue_rows, axis=1)
                            table_placeholder.dataframe(styled, use_container_width=True)

            if not st.session_state["ai_numeric_string_results"].empty:
                st.markdown("**Numeric/text consistency across interviews:**")
                df_final = st.session_state["ai_numeric_string_results"]
                styled_final = df_final.style.apply(highlight_issue_rows, axis=1)
                st.dataframe(styled_final, use_container_width=True)

                csv_num = df_final.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "Download numeric–string QC results as CSV",
                    data=csv_num,
                    file_name="qc_numeric_string_ai.csv",
                    mime="text/csv",
                    key="dl_ai_numeric_string_results",
                )

                update_ai_qc_summary(df_int)


                 # ----------------------------------------
        # 6. AI innovation string QC (Oslo Manual 2005) – row-by-row, unified table
        # ----------------------------------------
        st.markdown("---")
        st.markdown("### 6. AI innovation string QC (Oslo Manual 2005) – row-by-row")

        raw_df = st.session_state.get("raw_df")
        if raw_df is None:
            st.info("Upload raw Stata data (.dta) in the sidebar to enable this check.")
        else:
            st.session_state.setdefault("ai_innovation_qc_results", pd.DataFrame())

            idu_options_innov = sorted(df_int["idu"].astype(str).unique().tolist())
            idu_for_innov = st.multiselect(
                "Interviews to include in the innovation string QC",
                idu_options_innov,
                key="idu_innovation_qc",
            )

            use_all_innov = st.checkbox(
                "Use all interviews for innovation QC",
                value=False,
                key="chk_innovation_all",
            )

            if st.button("Run innovation QC (row-by-row)", key="btn_ai_innovation_qc_rowwise"):
                if not idu_for_innov and not use_all_innov:
                    st.warning("Select at least one interview or tick 'Use all interviews'.")
                else:
                    if use_all_innov:
                        selected_ids = idu_options_innov
                    else:
                        selected_ids = [str(x) for x in idu_for_innov]

                    unified_rows = []
                    progress = st.progress(0.0)
                    status = st.empty()
                    table_placeholder = st.empty()

                    total = len(selected_ids)
                    processed = 0

                    for i, idu_val in enumerate(selected_ids, start=1):
                        # 1) one-interview slice from raw_df
                        row_df = get_raw_rows_for_idu(raw_df, idu_val)
                        if row_df.empty:
                            continue

                        # respondent's own innovation flag
                        resp_flag = derive_respondent_innovation_flag(row_df)
                        idu_str = str(idu_val)
                        interview_id = get_primary_interview_id(row_df)

                        # 2) call AI on THIS interview only
                        report_text = ai_check_innovation_strings(row_df)
                        processed += 1

                        # 3) No descriptions case
                        if isinstance(report_text, str) and report_text.strip().startswith(
                            "No innovation descriptions detected in the sample"
                        ):
                            unified_rows.append(
                                {
                                    "idu": idu_str,
                                    "InterviewID": interview_id or idu_str,
                                    "RespondentFlag": resp_flag,
                                    "is_innovation": "",
                                    "TypeOfInnovation": "",
                                    "Notes": "No innovation descriptions detected in the sample",
                                }
                            )
                        else:
                            # 4) Parse the markdown table: | InterviewID | is_innovation | TypeOfInnovation | Notes |
                            parsed = parse_markdown_table(
                                report_text,
                                expected_first_headers=["InterviewID", "is_innovation"],
                            )
                            if not parsed:
                                # fallback: log raw text
                                unified_rows.append(
                                    {
                                        "idu": idu_str,
                                        "InterviewID": interview_id or idu_str,
                                        "RespondentFlag": resp_flag,
                                        "is_innovation": "",
                                        "TypeOfInnovation": "",
                                        "Notes": report_text.strip(),
                                    }
                                )
                            else:
                                for row_p in parsed:
                                    unified_rows.append(
                                        {
                                            "idu": idu_str,
                                            "InterviewID": row_p.get("InterviewID", interview_id or idu_str),
                                            "RespondentFlag": resp_flag,
                                            "is_innovation": row_p.get("is_innovation", ""),
                                            "TypeOfInnovation": row_p.get("TypeOfInnovation", ""),
                                            "Notes": row_p.get("Notes", ""),
                                        }
                                    )

                        # 5) live update table
                        df_res = pd.DataFrame(unified_rows)
                        st.session_state["ai_innovation_qc_results"] = df_res

                        progress.progress(i / total)
                        status.text(f"Processed {processed} / {total} interviews")

                        styled = df_res.style.apply(highlight_innovation_rows, axis=1)
                        table_placeholder.dataframe(styled, use_container_width=True)

            # final display
            if not st.session_state["ai_innovation_qc_results"].empty:
                st.markdown("**Innovation classification across interviews:**")
                df_final = st.session_state["ai_innovation_qc_results"]
                styled_final = df_final.style.apply(highlight_innovation_rows, axis=1)
                st.dataframe(styled_final, use_container_width=True)

                csv_innov = df_final.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "Download innovation QC results as CSV",
                    data=csv_innov,
                    file_name="qc_innovation_ai.csv",
                    mime="text/csv",
                    key="dl_ai_innovation_results",
                )

                update_ai_qc_summary(df_int)






                   # ----------------------------------------
               # ----------------------------------------
        # 7. AI skip and hard-check QC – row-by-row, unified table
        # ----------------------------------------
        st.markdown("---")
        st.markdown("### 7. AI skip and hard-check QC – row-by-row")

        raw_df = st.session_state.get("raw_df")
        codebook_data = st.session_state.get("codebook_data")

        if raw_df is None or not codebook_data:
            st.info(
                "Upload the raw Stata data (.dta) and Global Codebook (.xlsx) in the sidebar "
                "to enable skip / hard-check QC."
            )
        else:
            st.session_state.setdefault("ai_skip_qc_results", pd.DataFrame())

            idu_options_skip = sorted(df_int["idu"].astype(str).unique().tolist())
            idu_for_skip = st.multiselect(
                "Interviews to include in the skip / hard-check QC",
                idu_options_skip,
                key="idu_skip_qc",
            )

            use_all_skip = st.checkbox(
                "Use all interviews for skip / hard-check QC",
                value=False,
                key="chk_skip_all",
            )

            if st.button("Run skip / hard-check QC (row-by-row)", key="btn_ai_skip_qc_rowwise"):
                if not idu_for_skip and not use_all_skip:
                    st.warning("Select at least one interview or tick 'Use all interviews'.")
                else:
                    if use_all_skip:
                        selected_ids = idu_options_skip
                    else:
                        selected_ids = [str(x) for x in idu_for_skip]

                    unified_rows = []
                    progress = st.progress(0.0)
                    status = st.empty()
                    table_placeholder = st.empty()

                    total = len(selected_ids)
                    processed = 0

                    for i, idu_val in enumerate(selected_ids, start=1):
                        row_df = get_raw_rows_for_idu(raw_df, idu_val)
                        if row_df.empty:
                            continue

                        report_text = ai_check_skip_logic(row_df, codebook_data)
                        processed += 1
                        idu_str = str(idu_val)

                        if isinstance(report_text, str) and report_text.strip().startswith(
                            "No skip or hard-check violations detected in the sample."
                        ):
                            unified_rows.append(
                                {
                                    "idu": idu_str,
                                    "RuleSource": "",
                                    "RuleLabel": "",
                                    "RuleType": "",
                                    "Variables": "",
                                    "ApproxViolations": "",
                                    "ExampleInterviewIDs": "",
                                    "Notes": "No skip or hard-check violations detected in the sample.",
                                }
                            )
                        else:
                            parsed = parse_markdown_table(
                                report_text,
                                expected_first_headers=["RuleSource", "RuleLabel"],
                            )
                            if not parsed:
                                unified_rows.append(
                                    {
                                        "idu": idu_str,
                                        "RuleSource": "",
                                        "RuleLabel": "",
                                        "RuleType": "",
                                        "Variables": "",
                                        "ApproxViolations": "",
                                        "ExampleInterviewIDs": "",
                                        "Notes": report_text.strip(),
                                    }
                                )
                            else:
                                for row in parsed:
                                    unified_rows.append(
                                        {
                                            "idu": idu_str,
                                            "RuleSource": row.get("RuleSource", ""),
                                            "RuleLabel": row.get("RuleLabel", ""),
                                            "RuleType": row.get("RuleType", ""),
                                            "Variables": row.get("Variables", ""),
                                            "ApproxViolations": row.get("ApproxViolations", ""),
                                            "ExampleInterviewIDs": row.get("ExampleInterviewIDs", ""),
                                            "Notes": row.get("Notes", ""),
                                        }
                                    )

                        df_res = pd.DataFrame(unified_rows)
                        st.session_state["ai_skip_qc_results"] = df_res

                        progress.progress(i / total)
                        status.text(f"Processed {processed} / {total} interviews")

                        styled = df_res.style.apply(highlight_issue_rows, axis=1)
                        table_placeholder.dataframe(styled, use_container_width=True)

            if not st.session_state["ai_skip_qc_results"].empty:
                st.markdown("**Skip / hard-check violations across interviews:**")
                df_final = st.session_state["ai_skip_qc_results"]
                styled_final = df_final.style.apply(highlight_issue_rows, axis=1)
                st.dataframe(styled_final, use_container_width=True)

                csv_skip = df_final.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "Download skip / hard-check QC results as CSV",
                    data=csv_skip,
                    file_name="qc_skip_hard_ai.csv",
                    mime="text/csv",
                    key="dl_ai_skip_results",
                )

                update_ai_qc_summary(df_int)






                 # ----------------------------------------
        # 8. AI string QC (other specify & units) – row-by-row, unified table
        # ----------------------------------------
        st.markdown("---")
        st.markdown("### 8. AI string QC (other specify & units) – row-by-row")

        raw_df = st.session_state.get("raw_df")
        codebook_data = st.session_state.get("codebook_data")

        if raw_df is None:
            st.info("Upload the raw Stata data (.dta) in the sidebar to enable string QC.")
        else:
            st.session_state.setdefault("ai_string_qc_results", pd.DataFrame())

            idu_options_string = sorted(df_int["idu"].astype(str).unique().tolist())
            idu_for_string = st.multiselect(
                "Interviews to include in the string QC",
                idu_options_string,
                key="idu_string_qc",
            )

            use_all_string = st.checkbox(
                "Use all interviews for string QC",
                value=False,
                key="chk_string_all",
            )

            if st.button("Run string QC (row-by-row)", key="btn_ai_string_qc_rowwise"):
                if not idu_for_string and not use_all_string:
                    st.warning("Select at least one interview or tick 'Use all interviews'.")
                else:
                    if use_all_string:
                        selected_ids = idu_options_string
                    else:
                        selected_ids = [str(x) for x in idu_for_string]

                    unified_rows = []
                    progress = st.progress(0.0)
                    status = st.empty()
                    table_placeholder = st.empty()

                    total = len(selected_ids)
                    processed = 0

                    for i, idu_val in enumerate(selected_ids, start=1):
                        row_df = get_raw_rows_for_idu(raw_df, idu_val)
                        if row_df.empty:
                            continue

                        report_text = ai_check_string_qc(row_df, codebook_data)
                        processed += 1
                        idu_str = str(idu_val)

                        if isinstance(report_text, str) and report_text.strip().startswith(
                            "No string issues detected in the sample."
                        ):
                            unified_rows.append(
                                {
                                    "idu": idu_str,
                                    "Var": "",
                                    "InterviewID": "",
                                    "ExampleValue": "",
                                    "IssueType": "",
                                    "Notes": "No string issues detected in the sample.",
                                }
                            )
                        else:
                            parsed = parse_markdown_table(
                                report_text,
                                expected_first_headers=["Var", "InterviewID"],
                            )
                            if not parsed:
                                unified_rows.append(
                                    {
                                        "idu": idu_str,
                                        "Var": "",
                                        "InterviewID": "",
                                        "ExampleValue": "",
                                        "IssueType": "",
                                        "Notes": report_text.strip(),
                                    }
                                )
                            else:
                                for row in parsed:
                                    unified_rows.append(
                                        {
                                            "idu": idu_str,
                                            "Var": row.get("Var", ""),
                                            "InterviewID": row.get("InterviewID", ""),
                                            "ExampleValue": row.get("ExampleValue", ""),
                                            "IssueType": row.get("IssueType", ""),
                                            "Notes": row.get("Notes", ""),
                                        }
                                    )

                        df_res = pd.DataFrame(unified_rows)
                        st.session_state["ai_string_qc_results"] = df_res

                        progress.progress(i / total)
                        status.text(f"Processed {processed} / {total} interviews")

                        styled = df_res.style.apply(highlight_issue_rows, axis=1)
                        table_placeholder.dataframe(styled, use_container_width=True)

            if not st.session_state["ai_string_qc_results"].empty:
                st.markdown("**String issues across interviews:**")
                df_final = st.session_state["ai_string_qc_results"]
                styled_final = df_final.style.apply(highlight_issue_rows, axis=1)
                st.dataframe(styled_final, use_container_width=True)

                csv_str = df_final.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "Download string QC results as CSV",
                    data=csv_str,
                    file_name="qc_string_ai.csv",
                    mime="text/csv",
                    key="dl_ai_string_results",
                )

                update_ai_qc_summary(df_int)





                 # ----------------------------------------
        # 9. AI ISIC consistency QC (d1a1a, d1a1x, d1a2_v4) – row-by-row, unified table
        # ----------------------------------------
        st.markdown("---")
        st.markdown("### 9. AI ISIC consistency QC (d1a1a, d1a1x, d1a2_v4) – row-by-row")

        raw_df = st.session_state.get("raw_df")
        raw_df_labeled = st.session_state.get("raw_df_labeled")

        if raw_df is None:
            st.info(
                "Upload the raw Stata data (.dta) in the sidebar to enable ISIC consistency QC."
            )
        else:
            st.session_state.setdefault("ai_isic_qc_results", pd.DataFrame())

            idu_options_isic = sorted(df_int["idu"].astype(str).unique().tolist())
            idu_for_isic = st.multiselect(
                "Interviews to include in the ISIC consistency QC",
                idu_options_isic,
                key="idu_isic_qc",
            )

            use_all_isic = st.checkbox(
                "Use all interviews for ISIC QC",
                value=False,
                key="chk_isic_all",
            )

            if st.button("Run ISIC consistency QC (row-by-row)", key="btn_ai_isic_qc_rowwise"):
                if not idu_for_isic and not use_all_isic:
                    st.warning("Select at least one interview or tick 'Use all interviews'.")
                else:
                    if use_all_isic:
                        selected_ids = idu_options_isic
                    else:
                        selected_ids = [str(x) for x in idu_for_isic]

                    unified_rows = []
                    progress = st.progress(0.0)
                    status = st.empty()
                    table_placeholder = st.empty()

                    total = len(selected_ids)
                    processed = 0

                    for i, idu_val in enumerate(selected_ids, start=1):
                        row_df_num = get_raw_rows_for_idu(raw_df, idu_val)
                        if row_df_num.empty:
                            continue

                        row_df_lab = (
                            get_raw_rows_for_idu(raw_df_labeled, idu_val)
                            if raw_df_labeled is not None
                            else None
                        )

                        report_text = ai_check_isic_consistency(
                            row_df_num,
                            raw_df_labeled=row_df_lab,
                        )
                        processed += 1

                        idu_str = str(idu_val)
                        interview_id = get_primary_interview_id(row_df_num)

                        def _safe_val(df, col):
                            if df is None or df.empty or col not in df.columns:
                                return ""
                            v = df[col].iloc[0]
                            return "" if pd.isna(v) else str(v)

                        d1a1a_lbl = _safe_val(row_df_lab, "d1a1a") or _safe_val(row_df_num, "d1a1a")
                        d1a1x_val = _safe_val(row_df_num, "d1a1x")
                        d1a2_val = _safe_val(row_df_num, "d1a2_v4")

                        parsed = parse_markdown_table(
                            report_text,
                            expected_first_headers=["InterviewID", "d1a1a"],
                        )

                        if not parsed:
                            # Fallback: keep some info even if parsing fails
                            unified_rows.append(
                                {
                                    "idu": idu_str,
                                    "InterviewID": interview_id or idu_str,
                                    "d1a1a": d1a1a_lbl,
                                    "d1a1x": d1a1x_val,
                                    "d1a2_v4_vendor": d1a2_val,
                                    "AI_ISIC": "",
                                    "IssueType": "",
                                    "Notes": report_text.strip(),
                                }
                            )
                        else:
                            for row in parsed:
                                unified_rows.append(
                                    {
                                        "idu": idu_str,
                                        "InterviewID": row.get("InterviewID", interview_id or idu_str),
                                        "d1a1a": row.get("d1a1a", d1a1a_lbl),
                                        "d1a1x": row.get("d1a1x", d1a1x_val),
                                        "d1a2_v4_vendor": row.get("d1a2_v4_vendor", d1a2_val),
                                        "AI_ISIC": row.get("AI_ISIC", ""),
                                        "IssueType": row.get("IssueType", ""),
                                        "Notes": row.get("Notes", ""),
                                    }
                                )

                        df_res = pd.DataFrame(unified_rows)
                        st.session_state["ai_isic_qc_results"] = df_res

                        progress.progress(i / total)
                        status.text(f"Processed {processed} / {total} interviews")

                        if not df_res.empty:
                            styled = df_res.style.apply(highlight_issue_rows, axis=1)
                            table_placeholder.dataframe(styled, use_container_width=True)

            if not st.session_state["ai_isic_qc_results"].empty:
                st.markdown("**ISIC consistency across interviews:**")
                df_final = st.session_state["ai_isic_qc_results"]
                styled_final = df_final.style.apply(highlight_issue_rows, axis=1)
                st.dataframe(styled_final, use_container_width=True)

                csv_isic = df_final.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "Download ISIC QC results as CSV",
                    data=csv_isic,
                    file_name="qc_isic_ai.csv",
                    mime="text/csv",
                    key="dl_ai_isic_results",
                )

                update_ai_qc_summary(df_int)




with tab_report:
    st.subheader("QC report for TTL and vendor (AI-enhanced)")

    ai_df_int = st.session_state.get("ai_df_int")
    ai_stats = st.session_state.get("ai_qc_stats")

    if isinstance(ai_df_int, pd.DataFrame) and not ai_df_int.empty:
        base_int = ai_df_int
        use_ai = True
    else:
        base_int = df_int
        use_ai = False

    # Decide which cols to use
    reject_col = "ai_reject_decision" if use_ai and "ai_reject_decision" in base_int.columns else "reject_decision"
    score_col = "ai_qc_score" if use_ai and "ai_qc_score" in base_int.columns else "qc_score"

    # Basic metrics
    n_interviews = len(base_int)
    n_with_any_issue = int((base_int.get("ai_issue_total", base_int.get("any_issue", False)) > 0).sum())
    n_reject = int((base_int.get(reject_col, "") == "Reject").sum())
    n_review = int((base_int.get(reject_col, "") == "Review").sum())
    avg_score = float(base_int.get(score_col, pd.Series([np.nan])).mean())

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("# interviews", n_interviews)
        st.metric("# with any AI issue" if use_ai else "# with any QC issue", n_with_any_issue)
    with c2:
        st.metric("Avg QC score (AI)" if use_ai else "Avg QC score", f"{avg_score:.1f}" if not np.isnan(avg_score) else "n/a")
    with c3:
        st.metric("# recommended REJECT (AI)" if use_ai else "# recommended REJECT", n_reject)
        st.metric("# recommended REVIEW (AI)" if use_ai else "# recommended REVIEW", n_review)

    st.markdown("---")
    st.markdown("### Interviews recommended for rejection")

    reject_cols = [
        c
        for c in [
            "idu",
            "technicalid",
            INTERVIEWER_VAR if INTERVIEWER_VAR in base_int.columns else None,
            score_col,
            "ai_issue_total" if use_ai and "ai_issue_total" in base_int.columns else None,
            reject_col,
            "ai_reject_reasons" if use_ai and "ai_reject_reasons" in base_int.columns else "reject_reasons",
        ]
        if c is not None and c in base_int.columns
    ]

    df_reject = base_int[base_int.get(reject_col, "") == "Reject"][reject_cols].copy()

    if df_reject.empty:
        st.info("No interviews are currently classified as 'Reject' under the AI rules." if use_ai else
                "No interviews are currently classified as 'Reject' under the default rules.")
    else:
        st.dataframe(df_reject, use_container_width=True)
        csv_reject = df_reject.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download recommended rejections as CSV",
            data=csv_reject,
            file_name="qc_recommended_rejects_ai.csv" if use_ai else "qc_recommended_rejects.csv",
            mime="text/csv",
            key="dl_reject_csv_ai",
        )

    st.markdown("---")
    st.markdown("### Build combined Excel report (TTL + vendor)")

    raw_qc_vars = data.get("raw_qc_vars")
    raw_qc_int = data.get("raw_qc_interviews")

    if st.button("Generate Excel QC report", key="btn_build_excel_report"):
        report_buf = build_qc_excel_report(
            df_int=base_int,
            df_q_dyn=df_q_dyn,
            raw_qc_vars=raw_qc_vars,
            raw_qc_interviews=raw_qc_int,
        )
        st.session_state["qc_report_excel"] = report_buf.getvalue()

    if "qc_report_excel" in st.session_state:
        st.download_button(
            "Download QC report (Excel, TTL + vendor)",
            data=st.session_state["qc_report_excel"],
            file_name="WBES_QC_report_TTL_vendor_AI.xlsx" if use_ai else "WBES_QC_report_TTL_vendor.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dl_qc_report_excel_ai",
        )

        # After showing df_reject and CSV download:
    if use_ai and not df_reject.empty:
        st.markdown("---")
        st.markdown("### Push AI rejections to Survey Solutions")

        suso_client = st.session_state.get("suso_client")
        raw_df_for_reject = st.session_state.get("raw_df")

        if suso_client is None or raw_df_for_reject is None:
            st.info("SuSo client or raw data not available. Use the 'Survey Solutions API' data source in the sidebar first.")
        else:
            if st.button("Reject these interviews in SuSo (API)", key="btn_suso_push_rejects"):
                try:
                    success_ids, failed = push_ai_rejections_to_suso(
                        client=suso_client,
                        ai_df_int=base_int,   # base_int is ai_df_int when use_ai=True
                        raw_df=raw_df_for_reject,
                        use_ai=use_ai,
                    )
                    if success_ids:
                        st.success(f"Successfully sent reject command for {len(success_ids)} interviews: {', '.join(success_ids[:10])}{' ...' if len(success_ids) > 10 else ''}")
                    if failed:
                        st.error(
                            "Some interviews could not be rejected:\n"
                            + "\n".join(f"idu={idu}: {err}" for idu, err in failed)
                        )
                except Exception as e:
                    st.error(f"Failed to push rejections to SuSo: {e}")


    # ---------- AI-written narrative report ----------
    st.markdown("---")
    st.markdown("### AI-written narrative report (TTL + vendor)")

    if AI_CLIENT is None:
        st.info("AI narrative disabled (no API key).")
    else:
        st.session_state.setdefault("ai_full_report", "")

        if st.button("Generate narrative report (TTL + vendor)", key="btn_ai_full_report_ai"):
            # Build AI-focused context if available
            if use_ai and ai_stats:
                lines = []
                lines.append(f"AI-enhanced QC for {len(base_int)} interviews.")
                lines.append(
                    f"{ai_stats['n_ai_issues_any']} interviews have at least one AI-flagged issue; "
                    f"{ai_stats['n_ai_reject']} Reject, {ai_stats['n_ai_review']} Review."
                )
                if ai_stats["avg_ai_qc_score"] is not None:
                    lines.append(f"Average AI QC score: {ai_stats['avg_ai_qc_score']:.1f}.")

                # Top problem interviews
                worst = base_int.sort_values("ai_qc_score" if "ai_qc_score" in base_int.columns else score_col).head(15)
                cols_worst = [
                    "idu",
                    "technicalid",
                    score_col,
                    "ai_issue_total" if "ai_issue_total" in worst.columns else None,
                    "ai_numeric_issues" if "ai_numeric_issues" in worst.columns else None,
                    "ai_string_issues" if "ai_string_issues" in worst.columns else None,
                    "ai_isic_issues" if "ai_isic_issues" in worst.columns else None,
                    "ai_skip_issues" if "ai_skip_issues" in worst.columns else None,
                    "ai_innov_conflicts" if "ai_innov_conflicts" in worst.columns else None,
                    reject_col,
                    "ai_reject_reasons" if "ai_reject_reasons" in worst.columns else "reject_reasons",
                ]
                cols_worst = [c for c in cols_worst if c is not None and c in worst.columns]
                lines.append("WORST_INTERVIEWS_CSV:\n" + worst[cols_worst].to_csv(index=False))

                # AI issue tables (head)
                for key, label in [
                    ("ai_numeric_string_results", "AI_NUMERIC_STRING_CSV"),
                    ("ai_string_qc_results", "AI_STRING_QC_CSV"),
                    ("ai_isic_qc_results", "AI_ISIC_QC_CSV"),
                ]:
                    tbl = st.session_state.get(key)
                    if isinstance(tbl, pd.DataFrame) and not tbl.empty:
                        lines.append(f"{label} (head):\n" + tbl.head(50).to_csv(index=False))

                ctx = "\n\n".join(lines)
            else:
                # fallback: old rule-based context
                ctx = build_global_qc_context(df_int, df_q_dyn)

            question = (
                "Draft a concise QC report with two clear sections:\n"
                "1) TTL-facing executive summary: focus on AI-enhanced QC metrics (AI QC scores, AI Reject/Review, "
                "main types of AI-detected issues), and what they imply for data quality.\n"
                "2) Vendor-facing action list: specific instructions grouped by issue type (numeric/text mismatches, "
                "string issues, ISIC inconsistencies, skip/hard-check violations, innovation-flag conflicts), "
                "and guidance on how to correct or follow up on problematic interviews.\n\n"
                "Use ONLY the information in the context, do not hallucinate extra numbers. "
                "Be concrete and operational."
            )
            st.session_state["ai_full_report"] = ask_ai(
                ctx,
                question,
                max_output_tokens=80000,
                usage_label="ttl_vendor_report_ai",
            )

        if st.session_state["ai_full_report"]:
            st.markdown("**Narrative QC report (editable before sharing):**")
            st.text_area(
                "TTL + vendor QC report (AI-enhanced)",
                value=st.session_state["ai_full_report"],
                height=400,
                key="ai_full_report_textarea_ai",
            )
            st.session_state["ai_full_report"] = st.session_state["ai_full_report_textarea_ai"]

