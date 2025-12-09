"""
apply_historical_to_monthly.py

This script aggregates opening balances (Assets/Liabilities) with historical_variance
per month and writes cumulative balances into monthly_balance.

Behavior:
- For 2025-01: monthly_balance = opening_balance + historical_variance(2025-01)
- For each subsequent month M: monthly_balance(M) = monthly_balance(prev_M) + historical_variance(M)
- Only accounts whose `account_type` in `opening_balance` indicate Asset/ Liabilities are processed.
- Writes using MySQL INSERT ... ON DUPLICATE KEY UPDATE to avoid duplicates.

Run outside virtualenv (system Python) but requires packages: sqlalchemy, mysql-connector-python, pandas.
Install with (system Python):
    python -m pip install --user sqlalchemy mysql-connector-python pandas

Usage:
    python apply_historical_to_monthly.py

Make sure the DB connection string below matches your environment.
"""

from sqlalchemy import create_engine, text
import pandas as pd
import sys

# Configuration
DB_CONN = "mysql+mysqlconnector://root:appu1404@localhost/vera"
MONTHS = ['2025-01','2025-02','2025-03','2025-04','2025-05','2025-06','2025-07','2025-08','2025-09']

# Column names (adjust if your schema differs)
OPENING_BALANCE_TABLE = 'opening_balance'
HISTORICAL_TABLE = 'historical_variance'
MONTHLY_TABLE = 'monthly_balance'

# Helper: build safe IN-list from Python list
def sql_in_list(values):
    return ','.join(["'" + str(v).replace("'","''") + "'" for v in values])

# DB connection
engine = create_engine(DB_CONN)

with engine.connect() as conn:
    # Validate tables/columns exist
    required_tables = [OPENING_BALANCE_TABLE, HISTORICAL_TABLE, MONTHLY_TABLE]
    for t in required_tables:
        r = conn.execute(text("SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :t"), {'t': t}).fetchone()[0]
        if r == 0:
            print(f"Error: required table '{t}' not found in database. Aborting.")
            sys.exit(2)

    # Load opening balances (all account types)
    ob_q = text(f"SELECT account, opening_balance, account_type, description FROM {OPENING_BALANCE_TABLE}")
    ob_df_all = pd.read_sql(ob_q, conn)

    # Normalize account_type and split groups
    ob_df_all['acct_type_norm'] = ob_df_all['account_type'].astype(str).str.lower()
    mask_cum = ob_df_all['acct_type_norm'].str.contains('asset') | ob_df_all['acct_type_norm'].str.contains('liab')
    mask_noncum = ob_df_all['acct_type_norm'].str.contains('income') | ob_df_all['acct_type_norm'].str.contains('expens')

    cum_df = ob_df_all[mask_cum].copy()
    noncum_df = ob_df_all[mask_noncum].copy()

    accounts_cum = cum_df['account'].astype(str).tolist()
    accounts_noncum = noncum_df['account'].astype(str).tolist()

    if not accounts_cum and not accounts_noncum:
        print('No relevant accounts (Assets/Liabilities/Income/Expense) found in opening_balance. Nothing to do.')
        sys.exit(0)

    print(f'Processing {len(accounts_cum)} asset/liability accounts (cumulative) and {len(accounts_noncum)} income/expense accounts (non-cumulative)')

    # Function to fetch historical balances for a month as dict{account:balance}
    def fetch_historical_for_month(month_key, acct_list, use_smoothened=True):
        in_list = sql_in_list(acct_list)
        try:
            # For cumulative accounts we use smoothened_amount up to 2025-07
            if use_smoothened and month_key <= '2025-07':
                q = f"SELECT account, COALESCE(smoothened_amount,0) AS balance FROM {HISTORICAL_TABLE} WHERE month_key = '{month_key}' AND account IN ({in_list})"
                df = pd.read_sql(text(q), conn)
                return dict(zip(df['account'].astype(str), df['balance']))

            # For non-cumulative (income/expense) use journal_amount for months up to 2025-07
            if (not use_smoothened) and month_key <= '2025-07':
                q = f"SELECT account, COALESCE(journal_amount,0) AS balance FROM {HISTORICAL_TABLE} WHERE month_key = '{month_key}' AND account IN ({in_list})"
                df = pd.read_sql(text(q), conn)
                return dict(zip(df['account'].astype(str), df['balance']))

            # For 2025-08, compute sum(amount_original) from vouchers_curr_month for provided accounts
            if month_key == '2025-08':
                q = f"SELECT account_code AS account, COALESCE(SUM(amount_original),0) AS balance FROM vouchers_curr_month WHERE YEAR(transaction_date)=2025 AND MONTH(transaction_date)=8 AND account_code IN ({in_list}) GROUP BY account_code"
                df = pd.read_sql(text(q), conn)
                return dict(zip(df['account'].astype(str), df['balance']))

            # For 2025-09 and others return zeros (no historical change)
            return {}
        except Exception as e:
            print('Error fetching historical_variance/journal data:', e)
            sys.exit(3)

    # Function to fetch voucher amounts for a month (excluding BRTFWD, ROLCLR, OBTFER)
    def fetch_voucher_amounts_for_month(month_key, acct_list):
        in_list = sql_in_list(acct_list)
        try:
            # Extract year and month from month_key (format: YYYY-MM)
            year = int(month_key.split('-')[0])
            month = int(month_key.split('-')[1])
            
            # Use 'vouchers' table for 2025-01 to 2025-07, 'vouchers_curr_month' for 2025-08 onwards
            if month_key <= '2025-07':
                table_name = 'vouchers'
            else:
                table_name = 'vouchers_curr_month'
            
            q = f"""
                SELECT account_code AS account, COALESCE(SUM(amount_original),0) AS balance 
                FROM {table_name}
                WHERE YEAR(transaction_date) = {year} 
                  AND MONTH(transaction_date) = {month}
                  AND account_code IN ({in_list})
                  AND voucher NOT IN ('BRTFWD', 'ROLCLR', 'OBTFER')
                GROUP BY account_code
            """
            df = pd.read_sql(text(q), conn)
            return dict(zip(df['account'].astype(str), df['balance']))
        except Exception as e:
            print(f'Error fetching voucher amounts for {month_key}:', e)
            return {}

    # Function to fetch monthly_balance for a set of accounts and a month
    def fetch_monthly_for_month(month_key, acct_list):
        in_list = sql_in_list(acct_list)
        # monthly_balance stores values in column `opening_balance` so return it as 'balance'
        q = f"SELECT account, COALESCE(opening_balance,0) AS balance FROM {MONTHLY_TABLE} WHERE month_key = '{month_key}' AND account IN ({in_list})"
        try:
            df = pd.read_sql(text(q), conn)
            return dict(zip(df['account'].astype(str), df['balance']))
        except Exception as e:
            print('Error fetching monthly_balance:', e)
            sys.exit(4)

    # Upsert rows into monthly_balance
    def upsert_monthly_rows(rows):
        # rows: list of (account, month_key, balance)
        if not rows:
            return 0
        # monthly_balance table uses column `opening_balance` to store the monthly value
        # also store description and account_type
        # Do not overwrite existing non-empty description/account_type: only fill when NULL or empty
        insert_sql = (
            f"INSERT INTO {MONTHLY_TABLE} (account, month_key, opening_balance, description, account_type) "
            f"VALUES (:account, :month_key, :balance, :description, :account_type) "
            f"ON DUPLICATE KEY UPDATE opening_balance = VALUES(opening_balance), "
            f"description = IFNULL(NULLIF({MONTHLY_TABLE}.description, ''), VALUES(description)), "
            f"account_type = IFNULL(NULLIF({MONTHLY_TABLE}.account_type, ''), VALUES(account_type))"
        )
        try:
            conn.execute(text('START TRANSACTION'))
            conn.execute(text(insert_sql), rows)
            conn.execute(text('COMMIT'))
            return len(rows)
        except Exception as e:
            conn.execute(text('ROLLBACK'))
            print('Error upserting monthly rows:', e)
            sys.exit(5)

    # Prepare opening_balance map and metadata maps (from full opening_balance dataframe)
    ob_map = dict(zip(ob_df_all['account'].astype(str), ob_df_all['opening_balance'].fillna(0)))
    ob_desc_map = dict(zip(ob_df_all['account'].astype(str), ob_df_all.get('description', pd.Series([None]*len(ob_df_all))).astype(object)))
    ob_type_map = dict(zip(ob_df_all['account'].astype(str), ob_df_all.get('account_type', pd.Series([None]*len(ob_df_all))).astype(object)))

    # Month 1: opening_balance + historical(2025-01)
    # --- Process cumulative accounts (Assets/Liabilities) ---
    if accounts_cum:
        # For each month: opening_balance + sum of ALL voucher amounts from Jan through current month (cumulative)
        for i, month_key in enumerate(MONTHS):
            print(f'Processing {month_key} for cumulative accounts (Assets/Liabilities)')
            
            # Get cumulative sum of vouchers from first month through current month
            months_to_sum = MONTHS[:i+1]
            cumulative_voucher_map = {}
            
            for acct in accounts_cum:
                total_vouchers = 0
                for m in months_to_sum:
                    year = int(m.split('-')[0])
                    month_num = int(m.split('-')[1])
                    
                    # Use 'vouchers' table for 2025-01 to 2025-07, 'vouchers_curr_month' for 2025-08 onwards
                    if m <= '2025-07':
                        table_name = 'vouchers'
                    else:
                        table_name = 'vouchers_curr_month'
                    
                    voucher_sum_q = text(f"""
                        SELECT COALESCE(SUM(amount_original), 0) as total
                        FROM {table_name}
                        WHERE account_code = :acct 
                          AND YEAR(transaction_date) = :year 
                          AND MONTH(transaction_date) = :month
                          AND voucher NOT IN ('BRTFWD', 'ROLCLR', 'OBTFER')
                    """)
                    result = conn.execute(voucher_sum_q, {'acct': acct, 'year': year, 'month': month_num}).fetchone()
                    total_vouchers += float(result[0] or 0)
                
                cumulative_voucher_map[acct] = total_vouchers
            
            rows = []
            for acct in accounts_cum:
                ob_val = float(ob_map.get(acct, 0) or 0)
                voucher_val = cumulative_voucher_map.get(acct, 0)
                total = ob_val + voucher_val
                rows.append({'account': acct, 'month_key': month_key, 'balance': total,
                                'description': ob_desc_map.get(acct), 'account_type': ob_type_map.get(acct)})

            n = upsert_monthly_rows(rows)
            print(f'Inserted/Updated {n} rows for {month_key} (cumulative)')

    # --- Process non-cumulative accounts (Income/Expense) ---
    if accounts_noncum:
        for month_key in MONTHS:
            print(f'Processing {month_key} for non-cumulative accounts (Income/Expense)')
            
            # For non-cumulative accounts: opening_balance + voucher amounts for this month
            voucher_map = fetch_voucher_amounts_for_month(month_key, accounts_noncum)

            rows = []
            for acct in accounts_noncum:
                ob_val = float(ob_map.get(acct, 0) or 0)
                voucher_val = float(voucher_map.get(acct, 0) or 0)
                val = ob_val + voucher_val
                rows.append({'account': acct, 'month_key': month_key, 'balance': val,
                                'description': ob_desc_map.get(acct), 'account_type': ob_type_map.get(acct)})

            n = upsert_monthly_rows(rows)
            print(f'Inserted/Updated {n} rows for {month_key} (non-cumulative)')

print('All months processed successfully')
engine.dispose()
