import os
import re
import time
import secrets
from datetime import datetime
from pathlib import Path
from typing import Optional, Union

import pyodbc
import pandas as pd
import sqlparse  # Import for SQL formatting and splitting

try:
    from pandas import Styler
except ImportError:
    from pandas.io.formats.style import Styler

class DatabaseTablesComparator:
    """Compare DB tables and export results as parquet."""

    def __init__(
        self,
        db_type: Optional[str] = "TERA",
        customer_number: Optional[str] = None,
        file_generation_date: Optional[str] = "2023-05-08",
    ):
        self._teradata_user = "193911"
        self._sql_user = "sa"
        self._db_type = db_type
        # If customer_number is not provided, default to "ALL_CHECK"
        self._customer_number = customer_number if customer_number is not None else "ALL_CHECK"
        # Default file_generation_date to current date if not provided.
        self._file_generation_date = file_generation_date
        self._server = "DESKTOP-LSI1TPD"
        self._sql_password = "" # self._get_sql_password()  # Now obtaining password from environment
        self._sql_db = "AdventureWorks2022"
        self._dsn_name = "KHD_LIVE"
        self._connection = None
        self._logs = list()
        self._ile_value = 5

        self._workingdir = self._set_workingdir()
        if not self._workingdir:
            self._logger("CRITICAL ERROR: Could not set up working directory.")
            self._write_logs()
            raise SystemExit("Exiting the program...")

        self._scripts_path = os.path.join(self._workingdir, "SQLs")
        os.makedirs(self._scripts_path, exist_ok=True)
        self._parquets_path = os.path.join(self._workingdir, "PARQUETs")
        os.makedirs(self._parquets_path, exist_ok=True)
        self._xlsx_path = os.path.join(self._workingdir, "XLSXs")
        os.makedirs(self._xlsx_path, exist_ok=True)
        self._log_path = os.path.join(self._workingdir, "logs")
        os.makedirs(self._log_path, exist_ok=True)
        self._old_path = os.path.join(self._workingdir, "OLD")
        os.makedirs(self._old_path, exist_ok=True)

    def _get_sql_password(self, provided_password: Optional[str] = None) -> str:
        """Get DB password from argument or environment variable."""
        if provided_password is not None:
            return provided_password
        # Changed environment variable key to a conventional key.
        env_password = os.getenv("DB_AW22PASSWORD")
        if not env_password:
            self._logger("ERROR: DB_AW22PASSWORD not set in environment variables.")
            raise ValueError("Database password must be set in environment variables.")
        return env_password

    def _get_date_str(self, date_format: Optional[str] = "%Y-%m-%d %H:%M:%S") -> str:
        """Return current timestamp in the given format."""
        date_format = date_format or "%Y-%m-%d %H:%M:%S"
        return datetime.now().strftime(date_format)

    def _logger(self, message: str, level: str = "INFO") -> None:
        """Log a message with a timestamp and level."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"{timestamp} [{level}] - {message}"
        if len(self._logs) > 1000:
            self._logs.pop(0)
        self._logs.append(log_entry)
        print(log_entry)

    def _write_logs(self, max_attempts: int = 3, retry_delay: float = 2.0) -> None:
        """Save logs to a file with error handling."""
        log_filename = (
            f"I-O-I_{self._customer_number}_"
            f"{self._get_date_str('%Y%m%d%H%M%S')}_I-O-I.log"
        )
        log_file_path = os.path.join(self._log_path, log_filename)
        self._logger(f"Logs stored at: {log_file_path}")
        for attempt in range(1, max_attempts + 1):
            try:
                with open(log_file_path, "a", encoding="utf-8") as log_file:
                    for log_entry in self._logs[-500:]:
                        log_file.write(f"{log_entry}\n")
                self._logger("Logs written successfully.")
                break
            except (PermissionError, OSError) as e:
                self._logger(f"Attempt {attempt}/{max_attempts}: Failed to write logs: {e}")
            if attempt < max_attempts:
                time.sleep(retry_delay)
            else:
                print("CRITICAL: Could not write logs. Dumping to console:")
                for log_entry in self._logs[-100:]:
                    print(log_entry)

    def _set_workingdir(self) -> Optional[str]:
        """Set up the working directory safely."""
        workingdir = None
        try:
            if self._db_type == "TERA":
                workingdir = f"D:\\{self._teradata_user}\\_DHEAP\\DIFF"
            else:
                workingdir = f"C:\\{self._teradata_user}\\_CHEAP\\DIFF"
            os.makedirs(workingdir, exist_ok=True)
            abs_workingdir = os.path.abspath(workingdir)
            self._logger(f"SUCCESS: Working dir set to {abs_workingdir}.")
            return abs_workingdir
        except (FileNotFoundError, PermissionError, OSError) as e:
            self._logger(f"ERROR: Unable to set working dir '{workingdir}': {e}")
        return None

    def _open_connection(self) -> bool:
        """Open the database connection based on the db_type."""
        if self._connection:
            self._logger("WARNING: Connection already open.")
            return True
        try:
            if self._db_type == "TERA":
                self._logger(f"Opening Teradata DSN: {self._dsn_name}...")
                self._connection = pyodbc.connect(f"DSN={self._dsn_name}")
            else:
                self._logger(f"Opening SQL Server: {self._server}...")
                self._connection = pyodbc.connect(
                    f"DRIVER={{SQL Server}};"
                    f"SERVER={self._server};"
                    f"DATABASE={self._sql_db};"
                    f"UID={self._sql_user};"
                    f"PWD={self._sql_password};"
                )
            self._logger("SUCCESS: DB connection opened.")
            return True
        except (pyodbc.InterfaceError, pyodbc.DatabaseError,
                pyodbc.OperationalError, pyodbc.Error) as e:
            target = (
                f"Teradata DSN: {self._dsn_name}"
                if self._db_type == "TERA"
                else f"SQL Server: {self._server}"
            )
            self._logger(f"ERROR: Connection error for {target}: {e}")
        except Exception as e:
            target = (
                f"Teradata DSN: {self._dsn_name}"
                if self._db_type == "TERA"
                else f"SQL Server: {self._server}"
            )
            self._logger(f"ERROR: Unexpected error for {target}: {e}")
        return False

    def _close_connection(self) -> None:
        """Close the database connection safely."""
        if not self._connection:
            self._logger("WARNING: No active connection to close.")
            return
        try:
            self._connection.commit()
            self._logger("SUCCESS: Connection committed.")
        except pyodbc.Error as e:
            self._logger(f"WARNING: Commit failed: {e}")
        try:
            self._connection.close()
            self._logger("SUCCESS: Connection closed.")
        except pyodbc.Error as e:
            self._logger(f"ERROR: Close connection failed: {e}")
        finally:
            self._logger("INFO: Connection reset to None.")
            self._connection = None

    def _exit_program(self) -> None:
        """Exit the program gracefully (call explicitly if needed)."""
        self._logger("I-O-I SHUTTING DOWN THE PROGRAM", level="INFO")
        # Removed excessive delay for a more responsive shutdown.
        self._write_logs()
        print("W8ing 10 sec before exit()")
        time.sleep(10)
        raise SystemExit("Exiting the program...")

    def _create_tables(self) -> str:
        """Create volatile tables via SQL scripts."""
        start_time = time.time()
        # self._logger("INFO: Dropping existing volatile tables...")
        # self._drop_volatile_tables()
        script_map = {
            "TERA": (
                "_create_volatile_tables_ALL.sql"
                if self._customer_number == "ALL_CHECK"
                else "_create_volatile_tables_WH.sql"
            ),
            "SQL": "_create_rand_tables_SQL.sql",
        }
        script_file = script_map.get(self._db_type)
        if not script_file:
            return self._log_error(f"Invalid DB type: {self._db_type}")
        script_path = Path(self._scripts_path) / script_file
        if not script_path.exists():
            msg = f"SQL script not found: {script_path}"
            return self._log_error(msg)
        try:
            with script_path.open("r", encoding="utf-8") as file:
                sql_script = file.read().strip()
        except OSError as e:
            return self._log_error(f"Failed to read SQL script {script_path}: {e}")
        result = ""
        if self._db_type == "TERA":
            sql_script = self._replace_sql_parameters(sql_script)
            sql_script_cleaned = sqlparse.format(sql_script, strip_comments=True).strip()
            sql_statements = [stmt.strip() for stmt in sqlparse.split(sql_script_cleaned) if stmt.strip()]
            if not sql_statements:
                return self._log_error(f"SQL script {script_path} is empty or has only comments.")
            result = self._execute_sql_statements(sql_statements, script_path)
            self._logger(f"INFO: {result}")
            self._logger("INFO: SQL script loaded and preprocessed.")
        else:
            # For SQL branch, implementation can be added here.
            result = "SUCCESS: SQL branch execution not implemented."
            self._logger("INFO: SQL branch execution not implemented.")
        exec_time = round(time.time() - start_time, 2)
        self._logger(f"INFO: Execution time: {exec_time} sec.")
        return result

    def _replace_sql_parameters(self, sql_script: str) -> str:
        """Replace placeholders in the SQL script."""
        # Replace the date placeholder with the file generation date.
        sql_script = re.sub(
            r"\bDATE\s+YYYY-MM-DD\b", f"DATE '{self._file_generation_date}'", sql_script
        )
        
        # Replace __CUST_NUM__ with the customer number.
        try:
            if isinstance(self._customer_number, str):
                if self._customer_number.isdigit():
                    customer_number_val = int(self._customer_number)
                else:
                    customer_number_val = self._customer_number
            else:
                customer_number_val = self._customer_number

            sql_script = re.sub(r"__CUST_NUM__", str(customer_number_val), sql_script)
        except (ValueError, TypeError):
            # In case of error, fall back to using the original value.
            customer_number_val = self._customer_number
            sql_script = re.sub(r"__CUST_NUM__", str(customer_number_val), sql_script)
        
        # Optionally, replace __ILE__ with the specified value.
        # sql_script = re.sub(r"\b__ILE__\b", str(int(self._ile_value or 1)), sql_script)
        
        return sql_script

    def _execute_sql_statements(self, sql_statements: list, script_path: Path) -> str:
        """Execute SQL statements and handle errors."""
        executed, failed = 0, 0
        failed_statements = []
        try:
            with self._connection.cursor() as cursor:
                for index, statement in enumerate(sql_statements, start=1):
                    self._logger(f"DEBUG: Executing statement {index}: {statement[:50]}...")
                    try:
                        cursor.execute(statement)
                        self._connection.commit()  # Use connection commit
                        executed += 1
                        self._logger(f"SUCCESS: [Stmt {index}] Executed: {statement[:50]}...")
                    except (pyodbc.ProgrammingError, pyodbc.IntegrityError) as err:
                        self._logger(f"WARNING: [Stmt {index}] Error: {err}")
                        failed_statements.append((index, statement, str(err)))
                        failed += 1
                    except (pyodbc.OperationalError, pyodbc.DatabaseError) as err:
                        self._connection.rollback()
                        return self._log_error(f"FATAL: DB execution error: {err}")
            self._log_failed_statements(failed_statements, script_path)
            if failed == 0:
                self._logger(f"INFO: Executed {executed} statements successfully.")
                return f"SUCCESS: Executed {executed} statements."
            else:
                return self._log_error(f"Execution completed with {failed} failed statements.")
        except pyodbc.InterfaceError as e:
            return self._log_error(f"DB connection issue: {e}")
        except Exception as e:
            return self._log_error(f"Unexpected error: {e}")

    def _log_failed_statements(self, failed_statements: list, script_path: Path):
        """Log failed SQL statements for debugging."""
        if not failed_statements:
            return
        log_file = script_path.parent / "failed_statements.log"
        try:
            with open(log_file, "a", encoding="utf-8") as f:
                f.write("\n========================\n")
                f.write(f"Failed SQL Statements - {script_path.name}\n")
                for index, statement, error in failed_statements:
                    f.write(f"\n[Stmt {index}]: {statement}\n")
                    f.write(f"Error: {error}\n")
                f.write("\n========================\n")
            warn_msg = f"WARNING: {len(failed_statements)} failed statements logged to {log_file}"
            self._logger(warn_msg)
        except Exception as e:
            self._logger(f"ERROR: Failed to write log file {log_file}: {e}")

    def _log_error(self, message: str) -> str:
        """Log an error message and return it."""
        self._logger(f"ERROR: {message}")
        return f"ERROR: {message}"

    def _execute_final_sql(self, final_table: str = "#FINAL") -> pd.DataFrame:
        """Execute final SQL using a validated table name."""
        ALLOWED_TABLES = {"#FINAL"}
        if final_table not in ALLOWED_TABLES:
            raise ValueError(f"Invalid table name: {final_table}")
        sql = f"SELECT * FROM {final_table} ORDER BY DET_WH_CUST_NO, DET_ROW_NUM, ACC_WH_ACC_NO, ACC_REL_WH_ACC_NO;"
        if not self._connection:
            self._logger("ERROR: No active connection.")
            return pd.DataFrame()
        try:
            with self._connection.cursor() as cursor:
                self._logger(f"SQL Execution: {sql}")
                cursor.execute(sql)
                self._connection.commit()  # Use connection commit
                rows = cursor.fetchall()
                if not rows:
                    self._logger(f"No data in {final_table} table.")
                    return pd.DataFrame()
                columns = [col[0] for col in cursor.description]
                self._logger(f"SQL Execution SUCCESS: {sql}")
                return pd.DataFrame.from_records(rows, columns=columns)
        except pyodbc.DatabaseError as e:
            self._logger(f"SQL Execution ERROR: {e}")
            return pd.DataFrame()

    def _comparator_two_sided_merge(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Compare each DET_ROW_NUM to its successor (DET_ROW_NUM + 1) in a two-sided manner,
        highlighting missing rows and changed values. This version fixes the 'argument of type
        datetime.date is not iterable' error by converting values to strings before searching
        for '**' or '##'.
        """
        print("Original DataFrame sample:")
        print(df.head(10))

        # Sort the DataFrame for consistent grouping/output.
        sort_columns = [
            "DET_ROW_NUM", "DET_WH_CUST_NO", "ACC_WH_ACC_NO",
            "REL_WH_SECNDRY_CUST_NO", "REL_CUST_KIR_TYP_CDE"
        ]
        df = df.sort_values(by=sort_columns)

        # Grouping keys (excluding DET_ROW_NUM).
        id_columns = [
            "DET_WH_CUST_NO", "ACC_WH_ACC_NO", "REL_WH_SECNDRY_CUST_NO",
            "REL_CUST_KIR_TYP_CDE", "ACTE_PERIOD_DTE", "ACC_RPT_PERIOD_DTE",
            "ACC_PERIOD_DTE", "ACC_LOAD_DTE", "ACC_LOAD_TIME", "REL_RPT_PERIOD_DTE",
            "REL_PERIOD_DTE", "REL_LOAD_DTE", "REL_LOAD_TIME",
            "ACC_REL_RPT_PERIOD_DTE", "ACC_REL_PERIOD_DTE", "ACC_REL_LOAD_DTE",
            "ACC_REL_LOAD_TIME"
        ]

        # Columns defining the unique identity of a row within a DET_ROW_NUM.
        merge_key_cols = [
            "ACC_WH_ACC_NO", "REL_WH_SECNDRY_CUST_NO", "REL_CUST_KIR_TYP_CDE"
            # ... Add more columns if needed ...
        ]

        differences_list = []
        grouped = df.groupby(id_columns)

        # Helper to extract a dictionary from a merged row for one side.
        def build_side_dict(row, sub_df, suffix, ref_num):
            d = {}
            for col in sub_df.columns:
                # Merge key columns are not suffixed.
                if col in merge_key_cols:
                    d[col] = row[col]
                else:
                    col_name = col + suffix
                    if col_name in row and pd.notna(row[col_name]):
                        d[col] = row[col_name]
                    else:
                        d[col] = "##MISSING##"
            # Set the DET_ROW_NUM explicitly.
            d["DET_ROW_NUM"] = ref_num
            return d

        # Process each group.
        for group_keys, group_data in grouped:
            print(f"\nDEBUG: Processing group {group_keys} with {len(group_data)} row(s).")
            group_sorted = group_data.sort_values("DET_ROW_NUM").reset_index(drop=True)
            unique_nums = sorted(group_sorted["DET_ROW_NUM"].unique())
            group_max = max(unique_nums)

            # Compare each DET_ROW_NUM with its immediate successor.
            for i in range(len(unique_nums) - 1):
                old_num = unique_nums[i]
                new_num = unique_nums[i + 1]
                sub_old = group_sorted[group_sorted["DET_ROW_NUM"] == old_num].copy()
                sub_new = group_sorted[group_sorted["DET_ROW_NUM"] == new_num].copy()

                # Use merge indicator to see which side a row came from.
                merged = sub_old.merge(
                    sub_new,
                    on=merge_key_cols,
                    how="outer",
                    suffixes=('_old', '_new'),
                    indicator=True
                )

                for idx, row in merged.iterrows():
                    merge_status = row["_merge"]
                    if merge_status == "left_only":
                        # Row exists only in sub_old.
                        old_dict = build_side_dict(row, sub_old, "_old", old_num)
                        new_dict = {col: "##MISSING##" for col in sub_new.columns}
                        new_dict["DET_ROW_NUM"] = new_num
                    elif merge_status == "right_only":
                        # Row exists only in sub_new.
                        old_dict = {col: "##MISSING##" for col in sub_old.columns}
                        old_dict["DET_ROW_NUM"] = old_num
                        new_dict = build_side_dict(row, sub_new, "_new", new_num)
                    else:  # merge_status == "both"
                        old_dict = build_side_dict(row, sub_old, "_old", old_num)
                        new_dict = build_side_dict(row, sub_new, "_new", new_num)
                        # Compare common columns and highlight differences.
                        for col in old_dict:
                            if col == "DET_ROW_NUM":
                                continue
                            if col in new_dict:
                                val_old = old_dict[col]
                                val_new = new_dict[col]
                                # Highlight when one side is missing.
                                if val_old == "##MISSING##" and val_new != "##MISSING##":
                                    new_dict[col] = f"**{val_new}**"
                                elif val_new == "##MISSING##" and val_old != "##MISSING##":
                                    old_dict[col] = f"**{val_old}**"
                                # If both exist but differ.
                                elif val_old != val_new and "##" not in str(val_old) and "##" not in str(val_new):
                                    old_dict[col] = f"**{val_old}**"
                                    # Only style new row if not the final row.
                                    if new_num != group_max:
                                        new_dict[col] = f"**{val_new}**"

                    # Append the two rows (old and new) as a difference.
                    df_diff = pd.DataFrame([old_dict, new_dict])
                    df_diff["GROUP_KEYS"] = [group_keys, group_keys]
                    differences_list.append(df_diff)

        # If differences were found, write them to Excel.
        if differences_list:
            all_diffs_df = pd.concat(differences_list, ignore_index=True)

            # Helper to extract numeric values from DET_ROW_NUM (removing markers).
            def extract_num(val):
                val_str = str(val)
                for marker in ("**", "##"):
                    val_str = val_str.replace(marker, "")
                try:
                    return int(val_str)
                except ValueError:
                    return float("inf")

            all_diffs_df["DET_ROW_NUM_clean"] = all_diffs_df["DET_ROW_NUM"].apply(extract_num)
            for col in ["DET_WH_CUST_NO", "ACC_WH_ACC_NO", "REL_WH_SECNDRY_CUST_NO", "REL_CUST_KIR_TYP_CDE"]:
                if col in all_diffs_df.columns:
                    all_diffs_df[col + "_clean"] = all_diffs_df[col].apply(extract_num)

            sort_cols_clean = [
                "DET_ROW_NUM_clean", "DET_WH_CUST_NO_clean", "ACC_WH_ACC_NO_clean",
                "REL_WH_SECNDRY_CUST_NO_clean", "REL_CUST_KIR_TYP_CDE_clean"
            ]
            sort_cols_clean = [c for c in sort_cols_clean if c in all_diffs_df.columns]
            all_diffs_df.sort_values(by=sort_cols_clean, inplace=True)
            all_diffs_df.drop(columns=sort_cols_clean, inplace=True, errors="ignore")

            desired_order = [
                "DET_ROW_NUM", "DET_WH_CUST_NO", "ACC_WH_ACC_NO",
                "REL_WH_SECNDRY_CUST_NO", "REL_CUST_KIR_TYP_CDE"
            ]
            remaining_cols = [col for col in all_diffs_df.columns if col not in desired_order]
            new_order = desired_order + remaining_cols
            all_diffs_df = all_diffs_df[new_order]

            # Highlight differences.
            def highlight_differences(val):
                val_str = str(val)
                if "##" in val_str:
                    return "background-color: lightblue"
                elif "**" in val_str:
                    return "background-color: pink"
                return ""

            styled = all_diffs_df.style.applymap(highlight_differences)
            self._write_xlsx(styled)
            print(f"INFO: Differences have been saved to {self._xlsx_path}")
            return styled
        else:
            print("INFO: No differences found in any group. No Excel file generated.")
            return pd.DataFrame()
  
    def _write_parquet(self, df: pd.DataFrame) -> None:
        """Save a DataFrame as a parquet file."""
        try:
            if df.empty:
                self._logger("WARNING: Empty DataFrame; skipping Parquet.")
                return
            output_name = (
                f"{self._customer_number}_"
                f"ERR_DATE_{self._get_date_str('%Y%m%d')}_"
                f"{self._get_date_str('%Y%m%d%H%M%S')}_AM.parquet"
            )
            output_path = os.path.join(self._parquets_path, output_name)
            df.to_parquet(output_path, engine="pyarrow", compression="snappy")
            self._logger(f"Parquet stored at {output_path}")
        except Exception as e:
            self._logger(f"CRITICAL in _write_parquet(): {e}")
    
    def _write_xlsx(self, df: Union[pd.DataFrame, object]) -> None:
        """
        Save a DataFrame or a Styler to an Excel (XLSX) file.
        If 'df' is a Styler, its to_excel() method is called directly.
        """
        try:
            output_name = (
                f"WH_CUST_NO_{self._customer_number}_"
                f"{self._get_date_str('%Y%m%d%H%M%S')}_AM.xlsx"
            )
            output_name_diff = (
                f"DIFF_WH_CUST_NO_{self._customer_number}_"
                f"{self._get_date_str('%Y%m%d%H%M%S')}_AM.xlsx"
            )
            output_path = os.path.join(self._xlsx_path, output_name)
            output_path_diff = os.path.join(self._xlsx_path, output_name_diff)
            # Check if df is None before further processing.
            if df is None:
                self._logger("WARNING: Provided DataFrame/Styler is None; skipping XLSX.")
                return
            # Check the type via class name (to avoid AttributeError for pd.Styler)
            if type(df).__name__ == "Styler":
                df.to_excel(output_path_diff, index=False)
            else:
                if df.empty:
                    self._logger("WARNING: Empty DataFrame; skipping XLSX.")
                    return
                else:
                    df.to_excel(output_path, index=False)
            self._logger(f"Excel file stored at {output_path}")
        except Exception as e:
            self._logger(f"CRITICAL in _write_xlsx(): {e}")

    def print_attributes(self) -> None:
        """Print all instance attributes."""
        for key, value in vars(self).items():
            print(f"{key}: {value}")

    def run(self) -> bool:
        """Run the entire process."""
        try:
            self._logger("...INITIALIZATION...")
            if not self._open_connection():
                self._logger("DB connection failed. Exiting.")
                return False
            create_tables = self._create_tables()
            if "SUCCESS" not in create_tables:
                self._logger("ERROR: FAILED to create temp tables.")
                return False
            result_df = self._execute_final_sql()
            if result_df is None:
                result_df = pd.DataFrame()
            if not result_df.empty:
                if self._db_type == "TERA" and self._customer_number == "ALL_CHECK":
                    self._write_parquet(result_df)
                    self._comparator_two_sided_merge(result_df)
                    return True
                elif self._db_type == "TERA" and (
                    (isinstance(self._customer_number, str) and self._customer_number.isdigit())
                    or isinstance(self._customer_number, int)
                ):
                    self._write_xlsx(result_df)
                    self._comparator_two_sided_merge(result_df)
                    return True
                else:
                    pass  # implement for SQL later
            else:
                self._logger("WARNING: Empty DataFrame;")
                return False
        except Exception as e:
            self._logger(f"ERROR in run(): {e}")
            return False
        finally:
            self._logger("Process completed.")
            if self._connection:
                self._close_connection()
                self._exit_program()


# Example usage:
if __name__ == "__main__":
    #comparator = DatabaseTablesComparator(file_generation_date="2025-03-03")
    comparator = DatabaseTablesComparator(customer_number=428821167)
    #428496359)
    comparator.print_attributes()
    success = comparator.run()
    print(f"Run successful: {success}")

'''
CHANGELOG:
2024-03-13 -> 1.2.0_250313_beta was born;
2025-03-09 -> 1.1.3_250308_beta was born; FLAKE8, BANDIT -> PASS ✅
'''
