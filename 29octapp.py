import streamlit as st
import pandas as pd
import numpy as np
import re
import io
from datetime import datetime
from typing import List, Dict, Tuple

# Page config
st.set_page_config(page_title="KnowledgeExcel — Data Validation Automation (Final)", layout="wide")
st.title("KnowledgeExcel — Data Validation Automation (Final)")

st.markdown(
    "Flow: Generate Validation Rules → Download & Review (optional) → Upload revised rules (optional) → Confirm → Generate Validation Report. "
    "After generation, preview **Detailed Checks** and download both files (Rules + Report)."
)

# ---------------- Sidebar: uploads and controls ----------------
st.sidebar.header("Upload files")
raw_file = st.sidebar.file_uploader("Raw Data (Excel or CSV)", type=["xlsx", "xls", "csv"])
skips_file = st.sidebar.file_uploader("Sawtooth Skips (CSV/XLSX)", type=["csv", "xlsx"])
rules_template_file = st.sidebar.file_uploader("Optional: Validation Rules template (xlsx)", type=["xlsx"])
run_btn = st.sidebar.button("Run Full DV Automation: Build Validation Rules")

st.sidebar.markdown("---")
st.sidebar.header("Tuning parameters")
straightliner_threshold = st.sidebar.slider("Straightliner threshold", 0.50, 0.98, 0.85, 0.01)
junk_repeat_min = st.sidebar.slider("Junk OE: min repeated chars", 2, 8, 4, 1)
junk_min_length = st.sidebar.slider("Junk OE: min OE length", 1, 10, 2, 1)

# ---------------- xlsxwriter check ----------------
try:
    import xlsxwriter  # noqa: F401
    XLSXWRITER_AVAILABLE = True
except Exception:
    XLSXWRITER_AVAILABLE = False
    st.sidebar.warning("xlsxwriter not installed — Excel formatting will be basic. Add 'xlsxwriter' to requirements.txt for full formatting.")

# ---------------- Utility & cached readers ----------------
@st.cache_data(show_spinner=False)
def read_any_df_cached(uploaded_bytes: bytes, name: str):
    """Read a file from bytes. Return pandas DataFrame."""
    bio = io.BytesIO(uploaded_bytes)
    name = name.lower()
    try:
        if name.endswith((".xlsx", ".xls")):
            return pd.read_excel(bio, engine="openpyxl")
        else:
            return pd.read_csv(bio, encoding="utf-8-sig")
    except Exception:
        bio.seek(0)
        try:
            return pd.read_csv(bio, encoding="ISO-8859-1")
        except Exception:
            bio.seek(0)
            return pd.read_csv(bio, encoding="utf-8", errors="replace")

def read_any_df(uploaded):
    if uploaded is None:
        return None
    uploaded.seek(0)
    return read_any_df_cached(uploaded.read(), uploaded.name)

def is_probably_multiselect_block(vars_list: List[str], var: str) -> bool:
    """Detect pattern like Q1_1, Q1_2, Q1_3 — underscore + number suffix."""
    return bool(re.search(r'_[0-9]+$', var))

def is_probably_grid_block(vars_list: List[str], var: str) -> bool:
    """Detect patterns like R1,R2 or letter suffixes a,b,c or R1 style."""
    # grid heuristics: suffixes like R1,R2... or trailing letters a,b,c or numeric with letter prefix R
    if re.search(r'R\d+$', var, re.IGNORECASE):
        return True
    if re.search(r'[A-Za-z]$', var) and not re.search(r'_[0-9]+$', var):
        # trailing letter (a,b,c) - more likely rating/attribute item
        return True
    return False

def detect_variable_type_and_stats(series: pd.Series) -> Tuple[str, dict]:
    """
    Determine Variable_Type: 'Numeric', 'Categorical', 'Open-Ended', or 'Empty'
    Return (type, stats) where stats includes numeric counts, unique values, example tokens.
    """
    s = series.dropna()
    stats = {"n": len(series), "non_missing": len(s)}
    if len(s) == 0:
        return "Empty", stats

    # convert to string samples
    as_str = s.astype(str).str.strip()
    # proportion with alphabetic characters
    has_alpha = as_str.str.contains(r'[A-Za-z]', regex=True).mean()
    # proportion numeric
    coerced = pd.to_numeric(s, errors='coerce')
    numeric_prop = coerced.notna().mean()
    avg_len = as_str.str.len().mean()

    unique_vals = pd.Series(coerced.dropna().unique()).tolist()
    stats.update({"numeric_prop": numeric_prop, "has_alpha_prop": has_alpha, "avg_len": avg_len, "unique_numeric_vals": unique_vals, "unique_text_vals": as_str.unique()[:10].tolist()})

    # classification rules
    # Open-Ended if mostly non-numeric OR average length high OR alphabetic present
    if (has_alpha > 0.6) or (numeric_prop < 0.3 and avg_len > 10) or (avg_len > 30):
        return "Open-Ended", stats
    # Numeric if mostly numeric and small avg length
    if numeric_prop >= 0.6 and avg_len < 15:
        return "Numeric", stats
    # else consider categorical
    return "Categorical", stats

def group_variables(vars_list: List[str]) -> Dict[str, dict]:
    """
    Group variables into logical groups.
    Return mapping: prefix -> {"vars": [...], "group_type": "...", "label": "..."}
    """
    groups = {}
    for v in vars_list:
        # prefer prefix before last separator . or _
        if re.search(r'_[0-9]+$', v):
            # multi-select pattern Q1_1 -> prefix Q1
            prefix = re.sub(r'_[0-9]+$','', v)
            groups.setdefault(prefix, {"vars": [], "group_type": None}).get("vars").append(v)
        elif re.search(r'R\d+$', v, re.IGNORECASE):
            prefix = re.sub(r'R\d+$','', v)
            groups.setdefault(prefix, {"vars": [], "group_type": None}).get("vars").append(v)
        elif re.search(r'[A-Za-z]$', v) and len(v) > 2:
            # trailing letter like Q5a Q5b
            prefix = v[:-1]
            groups.setdefault(prefix, {"vars": [], "group_type": None}).get("vars").append(v)
        else:
            # standalone
            groups.setdefault(v, {"vars": [], "group_type": None}).get("vars").append(v)

    # Decide group_type for each prefix
    for prefix, info in groups.items():
        vars_in_group = info["vars"]
        # if any var had _# pattern -> Multi-Select Block
        if any(re.search(r'_[0-9]+$', vv) for vv in vars_in_group):
            info["group_type"] = f"Multi-Select Block ({prefix}, {len(vars_in_group)} items)"
        # else if R# or trailing letters -> Rating Grid
        elif any(re.search(r'R\d+$', vv, re.IGNORECASE) or re.search(r'[A-Za-z]$', vv) for vv in vars_in_group) and len(vars_in_group) > 1:
            info["group_type"] = f"Rating Grid ({prefix}, {len(vars_in_group)} items)"
        else:
            info["group_type"] = "Standalone"
    return groups

def find_next_valid_target(target_name: str, raw_cols: List[str]) -> Tuple[str, str]:
    """
    If target_name not found (e.g., text pages), find the next non-sys_ variable in raw_cols.
    Return (new_target_name, note). If not found, return ("", note).
    """
    # try to find index by approximate matching; if not found, try to find a 'next' by sequence
    try:
        # exact match first
        if target_name in raw_cols:
            return target_name, "Target exists"
        # try case-insensitive
        lower_map = {c.lower(): c for c in raw_cols}
        if target_name.lower() in lower_map:
            return lower_map[target_name.lower()], "Target matched case-insensitive"
    except Exception:
        pass

    # fallback: find first raw_col that occurs after the position of closest match in skips (not available here)
    # So choose the first non-sys variable in raw_cols as the conservative next
    for c in raw_cols:
        if not str(c).lower().startswith("sys_"):
            return c, f"Auto-adjusted to first data var: {c}"
    return "", "No valid data variable found to auto-adjust"


# ---------------- Helper functions for validation checks ----------------

def detect_junk_oe(value, junk_repeat_min=4, junk_min_length=2):
    if pd.isna(value):
        return False
    s = str(value).strip()
    if s == "":
        return True
    if s.isdigit() and len(s) <= 3:
        return True
    if re.match(r'^(.)\1{' + str(max(1, junk_repeat_min-1)) + r',}$', s):
        return True
    non_alnum_ratio = len(re.sub(r'[A-Za-z0-9]', '', s)) / max(1, len(s))
    if non_alnum_ratio > 0.6:
        return True
    if len(s) <= junk_min_length:
        return True
    return False


def find_straightliners(df, candidate_cols, threshold=0.85):
    straightliners = {}
    if len(candidate_cols) < 2:
        return straightliners
    m = df[candidate_cols].astype(str).fillna("")
    for idx, row in m.iterrows():
        non_blank = row.replace("", np.nan).dropna()
        if len(non_blank) < 2:
            continue
        vals = non_blank.values
        top_modes = pd.Series(vals).mode()
        if top_modes.empty:
            continue
        topval = top_modes.iloc[0]
        same_count = (vals == topval).sum()
        frac = same_count / len(non_blank)
        if frac >= threshold:
            straightliners[idx] = {
                "value": topval,
                "same_count": int(same_count),
                "total": int(len(non_blank)),
                "fraction": float(frac)
            }
    return straightliners


def parse_skip_expression_to_mask(expr, df):
    try:
        expr2 = expr.replace("AND", "&").replace("and", "&").replace("OR", "|").replace("or", "|")
        for col in df.columns:
            expr2 = re.sub(rf'\b{re.escape(col)}\b', f"df[{repr(col)}]", expr2)
        mask = eval(expr2, {"df": df, "np": np, "pd": pd})
        return mask.fillna(False).astype(bool)
    except Exception:
        return pd.Series(False, index=df.index)


# ---------------- Session state holders ----------------
if "rules_buf" not in st.session_state:
    st.session_state["rules_buf"] = None
if "final_vr_df" not in st.session_state:
    st.session_state["final_vr_df"] = None
if "rules_generated_time" not in st.session_state:
    st.session_state["rules_generated_time"] = None

# ---------------- Run rule generation ----------------
if run_btn:
    # Basic validations
    if raw_file is None or skips_file is None:
        st.error("Please upload both Raw Data and Sawtooth Skips files.")
    else:
        status = st.empty()
        progress = st.progress(0)
        status.text("Loading files...")
        raw_df = read_any_df(raw_file)
        skips_df = read_any_df(skips_file)
        progress.progress(10)

        # Detect respondent ID and remove BOM if present
        possible_ids = ["RESPID","RespondentID","CaseID","caseid","id","ID","Respondent Id","sys_RespNum"]
        id_col = next((c for c in raw_df.columns if c in possible_ids), raw_df.columns[0])
        id_col = id_col.lstrip("\ufeff")

        # Exclude system vars
        data_vars = [c for c in raw_df.columns if not str(c).lower().startswith("sys_")]

        status.text("Grouping variables and detecting types...")
        groups = group_variables(data_vars)

        # Precompute per-variable type and stats
        var_types = {}
        var_stats = {}
        for var in data_vars:
            vtype, stats = detect_variable_type_and_stats(raw_df[var])
            var_types[var] = vtype
            var_stats[var] = stats

        progress.progress(40)

        # Build validation rules list
        validation_rules = []
        DK_CODES = [88, 99]
        DK_TOKENS = ["DK", "Refused", "Don't know", "Dont know", "Refuse", "REFUSED"]

        # Parse skips file to extract skip rules (heuristics)
        skips_lc = {c.lower(): c for c in skips_df.columns}
        logic_col = next((skips_lc[c] for c in skips_lc if 'logic' in c or 'condition' in c), None)
        from_col = next((skips_lc[c] for c in skips_lc if 'skip from' in c or c == 'from' or 'question' in c), None)
        to_col = next((skips_lc[c] for c in skips_lc if 'skip to' in c or c == 'to' or 'target' in c), None)

        status.text("Building rules from Sawtooth Skips (auto-fix text page targets)...")
        # iterate skip rows and create rules; if skip target missing, auto-adjust to next valid var
        if logic_col:
            for _, r in skips_df.iterrows():
                logic = r.get(logic_col, "")
                src = r.get(from_col, "") if from_col else ""
                tgt = r.get(to_col, "") if to_col else ""
                if pd.isna(logic) or str(logic).strip() == "":
                    continue
                src_str = str(src).strip() if pd.notna(src) else ""
                tgt_str = str(tgt).strip() if pd.notna(tgt) else ""
                # ignore sys_ variables
                if src_str.lower().startswith("sys_"):
                    continue
                # Check if src present in data; if not, possibly the 'from' is a text page => try to find nearest
                if src_str not in raw_df.columns:
                    # try case-insensitive mapping
                    lower_map = {c.lower(): c for c in raw_df.columns}
                    if src_str.lower() in lower_map:
                        src_str = lower_map[src_str.lower()]
                # If target not present, auto-fix to next valid variable
                if tgt_str not in raw_df.columns:
                    new_tgt, note = find_next_valid_target(tgt_str, list(raw_df.columns))
                    if new_tgt:
                        description = f"Skip {src_str} when {logic} (Target {tgt_str} not in data; auto-adjusted → {new_tgt})"
                        tgt_str = new_tgt
                    else:
                        description = f"Skip {src_str} when {logic} (Target {tgt_str} not in data; NO auto-adjust found — review required)"
                else:
                    description = f"Skip {src_str} when {logic} (Target: {tgt_str})"

                # Only add rule if the source variable exists in our data vars
                if src_str in data_vars:
                    validation_rules.append({
                        "Variable": src_str,
                        "Variable_Type": var_types.get(src_str, ""),
                        "Group_Type": groups.get(re.sub(r'_[0-9]+$','',src_str), {}).get("group_type","Standalone"),
                        "Type": "Skip",
                        "Rule Applied": str(logic).strip(),
                        "Description": description,
                        "Derived From": "Sawtooth Skip"
                    })

        progress.progress(60)
        status.text("Adding smart auto-rules (Range, DK/Refused, Junk OE, Multi-Select groups)...")

        # Smart rule generation for each data variable (excluding sys_)
        for var in data_vars:
            vtype = var_types.get(var, "Categorical")
            stats = var_stats.get(var, {})
            # Group info
            prefix = re.sub(r'_[0-9]+$','', var) if re.search(r'_[0-9]+$', var) else (re.sub(r'R\d+$','',var) if re.search(r'R\d+$',var, re.IGNORECASE) else (var[:-1] if re.search(r'[A-Za-z]$', var) else var))
            group_info = groups.get(prefix, {"vars":[var], "group_type":"Standalone"})
            group_type = group_info.get("group_type", "Standalone")

            # If Open-Ended -> only Junk OE
            if vtype == "Open-Ended":
                validation_rules.append({
                    "Variable": var,
                    "Variable_Type": "Open-Ended",
                    "Group_Type": group_type,
                    "Type": "Junk OE",
                    "Rule Applied": "Junk-OE heuristics (repeats, too short, non-alnum heavy)",
                    "Description": "Open-ended: only junk OE detection applied",
                    "Derived From": "Auto"
                })
                # no DK/range rules for OE
                continue

            # Multi-Select group rules: applied at group level (we'll add identical entries for each var with group label)
            if re.search(r'_[0-9]+$', var):
                # Determine group members
                members = group_info.get("vars", [var])
                desc = f"Multi-select completeness & validity (group {prefix}, {len(members)} items). Values must be 0/1; no all-missing or all-0 respondent rows."
                validation_rules.append({
                    "Variable": var,
                    "Variable_Type": "Multi-Select",
                    "Group_Type": group_type,
                    "Type": "Multi-Select",
                    "Rule Applied": "Values must be 0/1; no all-missing/all-0 respondent rows",
                    "Description": desc,
                    "Derived From": "Auto"
                })
                # Also add DK rule only if DK tokens or codes present in data
                series = raw_df[var].dropna().astype(str).str.strip()
                present_tokens = [t for t in DK_TOKENS if any(series.str.lower() == t.lower())]  # exact match tokens
                present_codes = []
                try:
                    numeric_present = pd.to_numeric(raw_df[var], errors='coerce').dropna().astype(int).unique().tolist()
                    for code in DK_CODES:
                        if code in numeric_present:
                            present_codes.append(code)
                except Exception:
                    pass
                if present_tokens or present_codes:
                    validation_rules.append({
                        "Variable": var,
                        "Variable_Type": "Multi-Select",
                        "Group_Type": group_type,
                        "Type": "DK/Refused",
                        "Rule Applied": f"Codes {present_codes}; Tokens {present_tokens}",
                        "Description": "DK/Refused tokens/codes detected in multi-select (added only because present in data)",
                        "Derived From": "Auto"
                    })
                continue

            # For numeric/categorical variables not OE or multi-select:
            series = raw_df[var]
            coerced = pd.to_numeric(series, errors='coerce')
            numeric_vals = coerced.dropna().unique().tolist()

            # Determine if numeric rule should be added
            if len(numeric_vals) > 0:
                # variable contains numeric entries - create smart range
                if len(numeric_vals) == 1:
                    lo = hi = int(np.nanmin(coerced.dropna()))
                else:
                    lo = int(np.nanmin(coerced.dropna()))
                    hi = int(np.nanmax(coerced.dropna()))
                validation_rules.append({
                    "Variable": var,
                    "Variable_Type": "Numeric",
                    "Group_Type": group_type,
                    "Type": "Range",
                    "Rule Applied": f"{lo}-{hi}",
                    "Description": f"Numeric values expected between {lo} and {hi} based on data",
                    "Derived From": "Auto"
                })
                # DK/Refused only if found
                present_codes = [c for c in DK_CODES if c in [int(x) for x in numeric_vals if float(x).is_integer()]]
                series_text = series.dropna().astype(str).str.strip()
                present_tokens = [t for t in DK_TOKENS if any(series_text.str.lower() == t.lower())]
                if present_codes or present_tokens:
                    validation_rules.append({
                        "Variable": var,
                        "Variable_Type": "Numeric",
                        "Group_Type": group_type,
                        "Type": "DK/Refused",
                        "Rule Applied": f"Codes {present_codes}; Tokens {present_tokens}",
                        "Description": "DK/Refused tokens/codes detected in data (added only because present)",
                        "Derived From": "Auto"
                    })
                continue
            else:
                # No numeric values -> categorical. Add DK only if present in text values
                series_text = series.dropna().astype(str).str.strip()
                present_tokens = [t for t in DK_TOKENS if any(series_text.str.lower() == t.lower())]
                if present_tokens:
                    validation_rules.append({
                        "Variable": var,
                        "Variable_Type": "Categorical",
                        "Group_Type": group_type,
                        "Type": "DK/Refused",
                        "Rule Applied": f"Tokens {present_tokens}",
                        "Description": "DK/Refused tokens detected in categorical data (added only because present)",
                        "Derived From": "Auto"
                    })
                else:
                    # No rules necessary for simple categorical with no DK found
                    validation_rules.append({
                        "Variable": var,
                        "Variable_Type": "Categorical",
                        "Group_Type": group_type,
                        "Type": "None",
                        "Rule Applied": "",
                        "Description": "No automated rule (categorical with no DK tokens found)",
                        "Derived From": "Auto"
                    })
        progress.progress(90)

        # Build DataFrame and persist into session_state
        vr_df = pd.DataFrame(validation_rules)
        # Ensure ordering by data_vars order for readability
        def var_index(v):
            try:
                return data_vars.index(v)
            except Exception:
                return len(data_vars) + 1
        if not vr_df.empty:
            vr_df['__ord'] = vr_df['Variable'].apply(var_index)
            vr_df = vr_df.sort_values(['__ord']).drop(columns='__ord')
        else:
            vr_df = pd.DataFrame(columns=["Variable","Variable_Type","Group_Type","Type","Rule Applied","Description","Derived From"])

        # Persist rules in session as bytes for immediate download and for later use
        try:
            rules_buf = io.BytesIO()
            engine_choice = "xlsxwriter" if XLSXWRITER_AVAILABLE else "openpyxl"
            with pd.ExcelWriter(rules_buf, engine=engine_choice) as writer:
                vr_df.to_excel(writer, sheet_name="Validation_Rules", index=False)
                if XLSXWRITER_AVAILABLE:
                    workbook = writer.book
                    worksheet = writer.sheets["Validation_Rules"]
                    header_format = workbook.add_format({'bold': True, 'bg_color': '#305496', 'font_color': 'white', 'border':1})
                    for col_num, value in enumerate(vr_df.columns.values):
                        worksheet.write(0, col_num, value, header_format)
                    worksheet.freeze_panes(1,1)
                    for i, col in enumerate(vr_df.columns):
                        try:
                            width = max(vr_df[col].astype(str).map(len).max(), len(str(col))) + 2
                            worksheet.set_column(i, i, min(80, width))
                        except Exception:
                            pass
            rules_buf.seek(0)
            st.session_state["rules_buf"] = rules_buf.getvalue()
            st.session_state["final_vr_df"] = vr_df.copy()
            st.session_state["rules_generated_time"] = datetime.utcnow().isoformat()
            # Show preview and immediate download for review
            st.subheader("Validation Rules — Preview")
            st.dataframe(vr_df, use_container_width=True)
            st.download_button("📥 Download Validation Rules.xlsx (Generated)", data=io.BytesIO(st.session_state["rules_buf"]), file_name="Validation Rules.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            st.success("Validation rules generated and saved in session. You may download, review, and optionally upload a revised Validation Rules.xlsx before confirming.")
        except Exception as e:
            st.error("Failed to prepare Validation Rules file: " + str(e))
            st.session_state["rules_buf"] = None
            st.session_state["final_vr_df"] = vr_df.copy()

        progress.progress(100)

# Confirm & Generate flow, validation execution, report building, persistent downloads.

# Option to upload revised rules (handled in Part 1's uploader). Confirm + generate below.

# Confirm button (if not already created)
try:
    confirm_already = confirm_btn  # from Part 1 if present
except Exception:
    confirm_already = False

confirm_btn_2 = st.button("✅ Confirm & Generate Validation Report") if not confirm_already else False
if confirm_btn_2:
    st.session_state["_force_generate"] = True

# Allow a shortcut to generate if rules already exist
if st.session_state.get("_force_generate"):
    final_vr_df = st.session_state.get("final_vr_df")
    if final_vr_df is None:
        st.error("No Validation Rules available. Run 'Run Full DV Automation' first.")
    elif raw_file is None or skips_file is None:
        st.error("Raw data or skips file missing in current session. Re-run generation.")
    else:
        # ensure raw_df loaded
        raw_df = read_any_df(raw_file)
        data_vars = [c for c in raw_df.columns if not str(c).lower().startswith("sys_")]
        id_col = next((c for c in raw_df.columns if c in ["RESPID","RespondentID","CaseID","caseid","id","ID","Respondent Id","sys_RespNum"]), raw_df.columns[0])
        id_col = id_col.lstrip("\ufeff")
        status = st.empty()
        progress = st.progress(0)
        status.text("Running validation checks using confirmed rules...")
        progress.progress(5)

        detailed_findings = []
        data_df = raw_df.copy()

        def format_ids(ids_series, max_ids=200):
            return ";".join(map(str, ids_series.astype(str).unique()[:max_ids].tolist()))

        # Duplicate ID check
        dup_mask = data_df.duplicated(subset=[id_col], keep=False)
        if dup_mask.sum() > 0:
            detailed_findings.append({
                "Variable": id_col,
                "Check_Type": "Duplicate IDs",
                "Description": f"{int(dup_mask.sum())} duplicate rows (IDs duplicated)",
                "Affected_Count": int(dup_mask.sum()),
                "Respondent_IDs": format_ids(data_df.loc[dup_mask, id_col])
            })
        progress.progress(15)

        # Build masks and run checks per rule
        # Precompute multi-select groups
        groups = group_variables(data_vars)
        # For multi-select completeness and invalid values
        for prefix, info in groups.items():
            if not info["group_type"].startswith("Multi-Select"):
                continue
            members = info["vars"]
            # per-respondent rows
            block = data_df[members]
            # determine missing rows and all-zero rows
            all_missing_mask = block.isnull().all(axis=1)
            # consider values like '', ' ' as missing
            all_missing_mask = all_missing_mask | (block.applymap(lambda x: str(x).strip()=='').all(axis=1))
            all_zero_mask = (block.fillna(0).astype(str).applymap(lambda x: x.strip()).isin(['0','0.0','0.00','false','unchecked','False','FALSE','Unchecked','unchecked'])).all(axis=1)
            # invalid codes - anything not in allowed set 0/1/checked/unchecked/true/false
            allowed = set(['0','1','checked','unchecked','true','false','0.0','1.0'])
            invalid_mask = block.fillna('').astype(str).applymap(lambda x: x.strip().lower() not in allowed).any(axis=1)
            if all_missing_mask.sum() > 0:
                detailed_findings.append({
                    "Variable": prefix,
                    "Check_Type": "Multi-Select Completeness - All Missing",
                    "Description": f"{int(all_missing_mask.sum())} respondents with all values missing in multi-select group ({len(members)} items)",
                    "Affected_Count": int(all_missing_mask.sum()),
                    "Respondent_IDs": format_ids(data_df.loc[all_missing_mask, id_col])
                })
            if all_zero_mask.sum() > 0:
                detailed_findings.append({
                    "Variable": prefix,
                    "Check_Type": "Multi-Select Completeness - All Zero",
                    "Description": f"{int(all_zero_mask.sum())} respondents with all 0s in multi-select group ({len(members)} items)",
                    "Affected_Count": int(all_zero_mask.sum()),
                    "Respondent_IDs": format_ids(data_df.loc[all_zero_mask, id_col])
                })
            if invalid_mask.sum() > 0:
                detailed_findings.append({
                    "Variable": prefix,
                    "Check_Type": "Multi-Select Invalid Values",
                    "Description": f"{int(invalid_mask.sum())} respondents with invalid multi-select codes (not 0/1/checked/unchecked)",
                    "Affected_Count": int(invalid_mask.sum()),
                    "Respondent_IDs": format_ids(data_df.loc[invalid_mask, id_col])
                })
        progress.progress(35)

        # Apply rules in final_vr_df
        for _, rule in final_vr_df.iterrows():
            var = str(rule['Variable'])
            rtype = str(rule['Type']).strip().lower()
            r_applied = str(rule['Rule Applied'])
            if var not in data_df.columns:
                continue
            # Range
            if 'range' in rtype:
                m = re.match(r'^\s*(\d+)\s*[-:]\s*(\d+)\s*$', r_applied)
                lo, hi = 0, 999999
                if m:
                    lo, hi = int(m.group(1)), int(m.group(2))
                coerced = pd.to_numeric(data_df[var], errors='coerce')
                mask_out = (~coerced.isna()) & (~coerced.isin(DK_CODES)) & ((coerced < lo) | (coerced > hi))
                if mask_out.sum() > 0:
                    detailed_findings.append({
                        "Variable": var,
                        "Check_Type": "Range Violation",
                        "Description": f"{int(mask_out.sum())} values outside {lo}-{hi}",
                        "Affected_Count": int(mask_out.sum()),
                        "Respondent_IDs": format_ids(data_df.loc[mask_out, id_col])
                    })
            # Skip
            elif 'skip' in rtype:
                try:
                    mask = parse_skip_expression_to_mask(r_applied, data_df)
                    violators = data_df[mask & data_df[var].notna() & (data_df[var].astype(str).str.strip()!='')]
                    if len(violators) > 0:
                        detailed_findings.append({
                            "Variable": var,
                            "Check_Type": "Skip Violation",
                            "Description": f"{len(violators)} respondents answered {var} though skip ({r_applied}) applies",
                            "Affected_Count": int(len(violators)),
                            "Respondent_IDs": format_ids(violators[id_col])
                        })
                except Exception as e:
                    detailed_findings.append({
                        "Variable": var,
                        "Check_Type": "Skip Parsing Error",
                        "Description": f"Could not parse skip rule: {r_applied}. Error: {e}",
                        "Affected_Count": 0,
                        "Respondent_IDs": ""
                    })
            # DK/Refused
            elif 'dk' in rtype or 'ref' in rtype:
                s = data_df[var].astype(str)
                coerced = pd.to_numeric(data_df[var], errors='coerce')
                mask = s.str.strip().str.lower().isin([t.lower() for t in DK_TOKENS]) | coerced.isin(DK_CODES)
                if mask.sum() > 0:
                    detailed_findings.append({
                        "Variable": var,
                        "Check_Type": "DK/Refused",
                        "Description": f"{int(mask.sum())} DK/Refused occurrences",
                        "Affected_Count": int(mask.sum()),
                        "Respondent_IDs": format_ids(data_df.loc[mask, id_col])
                    })
            # Junk OE
            elif 'junk' in rtype or 'open' in rtype or 'oe' in rtype:
                series = data_df[var]
                mask = series.apply(lambda x: detect_junk_oe(x, junk_repeat_min, junk_min_length))
                if mask.sum() > 0:
                    detailed_findings.append({
                        "Variable": var,
                        "Check_Type": "Junk OE",
                        "Description": f"{int(mask.sum())} open-end responses flagged as junk",
                        "Affected_Count": int(mask.sum()),
                        "Respondent_IDs": format_ids(data_df.loc[mask, id_col])
                    })
            # Multi-Select & None are handled above or ignored here
        progress.progress(70)

        # Straightliner detection for rating grids
        prefixes = {}
        for v in data_vars:
            p = re.split(r'[_\.]', v)[0]
            prefixes.setdefault(p, []).append(v)
        for prefix, cols in prefixes.items():
            # determine if group is rating grid (based on groups computed earlier)
            gi = groups.get(prefix, {})
            if gi and gi.get("group_type","").startswith("Rating Grid"):
                sliners = find_straightliners(data_df, gi.get("vars", cols), threshold=straightliner_threshold)
                if sliners:
                    idxs = list(sliners.keys())
                    detailed_findings.append({
                        "Variable": prefix,
                        "Check_Type": "Straightliner (Grid)",
                        "Description": f"{len(sliners)} respondents flagged as straightliners across {len(gi.get('vars', cols))} items",
                        "Affected_Count": int(len(sliners)),
                        "Respondent_IDs": format_ids(pd.Series(idxs))
                    })
        progress.progress(90)

        # Build final DataFrames
        detailed_df = pd.DataFrame(detailed_findings) if detailed_findings else pd.DataFrame(columns=["Variable","Check_Type","Description","Affected_Count","Respondent_IDs"])
        summary_df = detailed_df.groupby("Check_Type", as_index=False)["Affected_Count"].sum().sort_values("Affected_Count", ascending=False) if not detailed_df.empty else pd.DataFrame(columns=["Check_Type","Affected_Count"])
        project_info = pd.DataFrame({
            "Report Generated":[datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")],
            "Raw Data Rows":[raw_df.shape[0]],
            "Raw Data Columns":[raw_df.shape[1]],
            "Respondent ID":[id_col],
            "Variables Validated":[len(data_vars)]
        })

        # Persist rules_buf if not present
        if st.session_state.get("rules_buf") is None and final_vr_df is not None:
            try:
                buf_r = io.BytesIO()
                engine_choice = "xlsxwriter" if XLSXWRITER_AVAILABLE else "openpyxl"
                with pd.ExcelWriter(buf_r, engine=engine_choice) as writer:
                    final_vr_df.to_excel(writer, sheet_name="Validation_Rules", index=False)
                buf_r.seek(0)
                st.session_state["rules_buf"] = buf_r.getvalue()
            except Exception:
                pass

        # Create report_buf
        report_buf = io.BytesIO()
        try:
            engine_choice = "xlsxwriter" if XLSXWRITER_AVAILABLE else "openpyxl"
            with pd.ExcelWriter(report_buf, engine=engine_choice) as writer:
                detailed_df.to_excel(writer, sheet_name="Detailed Checks", index=False)
                summary_df.to_excel(writer, sheet_name="Summary", index=False)
                final_vr_df.to_excel(writer, sheet_name="Validation_Rules", index=False)
                project_info.to_excel(writer, sheet_name="Project Info", index=False)
                if XLSXWRITER_AVAILABLE:
                    workbook = writer.book
                    header_fmt = workbook.add_format({'bold': True, 'bg_color': '#305496', 'font_color': 'white', 'border':1})
                    sheet_map = {"Detailed Checks": detailed_df, "Summary": summary_df, "Validation_Rules": final_vr_df, "Project Info": project_info}
                    for sheet_name, df_sheet in sheet_map.items():
                        try:
                            ws = writer.sheets[sheet_name]
                            ws.freeze_panes(1,1)
                            for col_num, value in enumerate(df_sheet.columns.values):
                                ws.write(0, col_num, value, header_fmt)
                            for i, col in enumerate(df_sheet.columns):
                                try:
                                    width = max(df_sheet[col].astype(str).map(len).max(), len(str(col))) + 2
                                    ws.set_column(i, i, min(80, width))
                                except Exception:
                                    pass
                        except Exception:
                            pass
            report_buf.seek(0)
            st.session_state["report_buf"] = report_buf.getvalue()
            st.session_state["detailed_df_preview"] = detailed_df.copy()
            st.success("Validation Report generated and saved in session.")
        except Exception as e:
            st.error("Could not prepare Validation Report: " + str(e))
            st.session_state["report_buf"] = None

# ---------------- Display Detailed Checks preview and persistent downloads ----------------
if st.session_state.get("detailed_df_preview") is not None:
    st.subheader("Detailed Checks — Preview (first 200 rows)")
    try:
        st.dataframe(st.session_state["detailed_df_preview"].head(200), use_container_width=True)
    except Exception:
        st.write(st.session_state["detailed_df_preview"].head(200))
    cols = st.columns(2)
    with cols[0]:
        if st.session_state.get("rules_buf") is not None:
            st.download_button("📥 Download Validation Rules.xlsx", data=io.BytesIO(st.session_state["rules_buf"]), file_name="Validation Rules.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        else:
            st.info("Validation Rules file not available for download.")
    with cols[1]:
        if st.session_state.get("report_buf") is not None:
            st.download_button("📥 Download Validation Report.xlsx", data=io.BytesIO(st.session_state["report_buf"]), file_name="Validation Report.xlsx", mime="application/vnd.openxmlformats-officedocument-spreadsheetml.sheet")
        else:
            st.info("Validation Report file not available for download yet.")

# EOF
