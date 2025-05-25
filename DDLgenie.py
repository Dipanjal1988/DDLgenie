import streamlit as st

import pandas as pd

import json

import io



# --- App Setup ---

st.set_page_config(page_title="ICS-DDLgenie", layout="wide")

st.title("ICS-DDLgenie")



# --- Login ---

if "authenticated" not in st.session_state:

    st.session_state.authenticated = False



if not st.session_state.authenticated:

    st.header("Login to continue")

    with st.form("login_form"):

        password = st.text_input("Enter Password", type="password")

        login_submit = st.form_submit_button("Submit")

        if login_submit:

            if password == "icsddg2025":

                st.session_state.authenticated = True

                st.success("Login successful.")

            else:

                st.error("Incorrect password.")

    st.stop()



# --- Helper Functions ---

def infer_dtype(value):

    try:

        int(value)

        return "INT64"

    except:

        try:

            float(value)

            return "FLOAT64"

        except:

            try:

                pd.to_datetime(value)

                return "TIMESTAMP"

            except:

                return "STRING"



def infer_column_types(df_sample):

    types = {}

    for col in df_sample.columns:

        inferred = df_sample[col].apply(infer_dtype)

        dtype = inferred.mode()[0]

        nullable = df_sample[col].isnull().any()

        types[col] = {"type": dtype, "nullable": nullable}

    return types



def generate_create_table_ddl(table_name, schema_dict, partition_col=None, cluster_cols=None):

    ddl = f"CREATE OR REPLACE TABLE `{table_name}` (\n"

    col_defs = []

    for col, info in schema_dict.items():

        null_flag = "" if info["nullable"] else " NOT NULL"

        col_defs.append(f"  `{col}` {info['type']}{null_flag}")

    ddl += ",\n".join(col_defs) + "\n)"

    if partition_col:

        ddl += f"\nPARTITION BY `{partition_col}`"

    if cluster_cols:

        ddl += f"\nCLUSTER BY {', '.join([f'`{c}`' for c in cluster_cols])}"

    ddl += ";"

    return ddl



def generate_external_table_ddl(table_name, gcs_uri):

    return f"""CREATE OR REPLACE EXTERNAL TABLE `{table_name}`

OPTIONS (

  format = 'CSV',

  uris = ['{gcs_uri}'],

  skip_leading_rows = 1

);"""



def generate_insert_sql(target_table, ext_table):

    return f"INSERT INTO `{target_table}` SELECT * FROM `{ext_table}`;"



def generate_drop_sql(ext_table):

    return f"DROP EXTERNAL TABLE `{ext_table}`;"



# --- Upload UI ---

st.subheader("Upload CSV, JSON, or Excel File for DDL Generation")



with st.form("ddl_form"):

    file = st.file_uploader("Upload your file", type=["csv", "json", "xlsx"])

    table_name = st.text_input("Target BigQuery Table (e.g., project.dataset.table)")

    gcs_path = st.text_input("GCS URI (for history load, optional)")

    partition_col = st.text_input("Partition Column (optional)")

    cluster_input = st.text_input("Cluster Columns (comma-separated, optional)")

    generate_btn = st.form_submit_button("Generate")



# --- Main Logic ---

if generate_btn and file and table_name:

    ext = file.name.split('.')[-1].lower()

    cluster_cols = [c.strip() for c in cluster_input.split(',')] if cluster_input else []



    if ext == "csv":

        df = pd.read_csv(file)

    elif ext == "xlsx":

        df = pd.read_excel(file)

    elif ext == "json":

        try:

            data = json.load(file)

            schema = data.get("schema")

            if not schema:

                st.error("JSON must contain a top-level 'schema' dictionary.")

                st.stop()

            parsed_schema = {k: {"type": v, "nullable": True} for k, v in schema.items()}

            ddl = generate_create_table_ddl(table_name, parsed_schema, partition_col, cluster_cols)

            st.subheader("Generated DDL")

            st.code(ddl, language="sql")

            st.download_button("Download DDL", ddl, file_name="create_table.sql", mime="text/sql")

            st.stop()

        except Exception as e:

            st.error(f"Error parsing JSON: {e}")

            st.stop()

    else:

        st.error("Unsupported file format.")

        st.stop()



    schema = infer_column_types(df.head(3))

    ddl = generate_create_table_ddl(table_name, schema, partition_col, cluster_cols)



    st.subheader("Generated DDL")

    st.code(ddl, language="sql")

    st.download_button("Download DDL", ddl, file_name="create_table.sql", mime="text/sql")



    if gcs_path:

        ext_table = f"{table_name}_ext"

        ext_ddl = generate_external_table_ddl(ext_table, gcs_path)

        insert_sql = generate_insert_sql(table_name, ext_table)

        drop_sql = generate_drop_sql(ext_table)



        st.subheader("History Load SQL")

        st.code(ext_ddl, language="sql")

        st.code(insert_sql, language="sql")

        st.code(drop_sql, language="sql")



        full_script = f"{ddl}\n\n-- External Table\n{ext_ddl}\n\n-- Insert\n{insert_sql}\n\n-- Drop\n{drop_sql}"

        st.download_button("Download Full History Load Script", full_script, file_name="history_load.sql", mime="text/sql")

else:

    if generate_btn:

        st.warning("Please upload a valid file and provide a table name.")