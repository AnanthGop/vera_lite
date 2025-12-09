import pandas as pd
from sqlalchemy import create_engine, text
import sys

# Config
EXCEL_PATH = 'files\\VSA TB August 2025_with_bold_flag.xlsx'
SHEET_NAME = None  # None -> active sheet
TABLE_NAME = 'opening_balance'
DB_CONN = "mysql+mysqlconnector://root:appu1404@localhost/vera"

# Mapping: try to map Excel column names to table columns
# Adjust these keys if your sheet has different column headers
# This script will attempt to find columns case-insensitively.
PREFERRED_COLUMNS = {
    'account': ['account', 'Account', 'Account Code', 'account_code'],
    'description': ['description', 'Description', 'account_desc', 'Account Description'],
    'opening_balance': ['opening_balance', 'Opening Balance', 'opening balance', 'amount'],
    'account_type': ['account_type', 'Account Type', 'type']
}


def find_column(df_cols, candidates):
    cols_lower = {c.lower(): c for c in df_cols}
    for cand in candidates:
        if cand.lower() in cols_lower:
            return cols_lower[cand.lower()]
    return None


def build_df_for_db(df):
    # Identify columns
    cols = df.columns.tolist()
    mapped = {}
    for target, candidates in PREFERRED_COLUMNS.items():
        found = find_column(cols, candidates)
        if found:
            mapped[target] = found
    
    # Create final dataframe with only the target columns (or fill missing)
    out = pd.DataFrame()
    for target in ['account', 'description', 'opening_balance', 'account_type']:
        if target in mapped:
            out[target] = df[mapped[target]]
        else:
            out[target] = None

    # Clean up whitespace
    out['account'] = out['account'].astype(str).str.strip()
    out['description'] = out['description'].astype(object)
    out['account_type'] = out['account_type'].astype(object)

    # Convert numeric opening_balance safely
    try:
        out['opening_balance'] = pd.to_numeric(out['opening_balance'], errors='coerce')
    except Exception:
        out['opening_balance'] = None

    return out


def main():
    print(f"Loading Excel: {EXCEL_PATH}")
    try:
        # The sheet has headers on the second row (row index 1) so read with header=1
        if SHEET_NAME:
            df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME, header=1, engine='openpyxl')
        else:
            df = pd.read_excel(EXCEL_PATH, header=1, engine='openpyxl')
    except FileNotFoundError:
        print('Excel file not found:', EXCEL_PATH, file=sys.stderr)
        sys.exit(2)
    except Exception as e:
        print('Error reading Excel:', e, file=sys.stderr)
        sys.exit(3)

    print('Columns in sheet:', df.columns.tolist())
    out_df = build_df_for_db(df)
    print('Prepared DataFrame columns for DB:', out_df.columns.tolist())

    # Drop rows without account
    before = len(out_df)
    out_df = out_df[out_df['account'].notna() & (out_df['account'].astype(str).str.strip() != '')]
    after = len(out_df)
    print(f'Rows before filter: {before}, after dropping empty account: {after}')

    if after == 0:
        print('No rows to insert. Exiting.')
        return

    engine = create_engine(DB_CONN)
    with engine.connect() as conn:
        # Optional: create temp table or upsert logic. For now append.
        print(f'Inserting {len(out_df)} rows into `{TABLE_NAME}` (append)')
        try:
            out_df.to_sql(TABLE_NAME, conn, if_exists='append', index=False)
            print('Insert completed successfully')
        except Exception as e:
            print('Error inserting into DB:', e, file=sys.stderr)
            sys.exit(4)

    engine.dispose()

if __name__ == '__main__':
    main()
