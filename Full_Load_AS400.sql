CREATE OR REPLACE PROCEDURE RENTALDB.STAGING.PROCESS_RENTAL_DATA()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'pandas')
HANDLER = 'main'
AS
$$
import json

import pandas as pd
from snowflake.snowpark.context import get_active_session

# casting informer datatypes to snowflake compatible datatypes
def build_cast_sql(table_name, column_config):
    dtype_map = {
        "CHAR": "VARCHAR",
        "DECIMAL": "NUMBER",
        "BIGINT": "NUMBER",
        "NUMERIC": "NUMBER",
        "DATE": "DATE",
        "BOOLEAN":"BOOLEAN",
        "TIMESTAMP":"TIMESTAMP_TZ"
    }

    cast_exprs = []
    for col, props in column_config.items():
        informer_dtype = props.get("type")
        length = props.get("length")
        scale = props.get("numeric_scale")
        snowflake_dtype = dtype_map.get(informer_dtype.upper(), informer_dtype)

        if informer_dtype.upper() == "DECIMAL":
            expr = f"CAST({col} AS {snowflake_dtype}({length},{scale})) AS {col}"
        elif informer_dtype.upper() == "CHAR":
            expr = f"CAST(TRIM({col}) AS {snowflake_dtype}({length})) AS {col}"
        elif informer_dtype.upper() == "TIMESTMP":
            expr = f"{col}"
        elif informer_dtype.upper() in ["DATE", "BOOLEAN", "TIMESTAMP"]:
            expr = f"CAST({col} AS {snowflake_dtype}) AS {col}"
        else:
            expr = f"CAST({col} AS {snowflake_dtype}({length})) AS {col}"

        cast_exprs.append(expr)

    return ", ".join(cast_exprs)

# function to replace leading or trailing underscores in column names:
def clean_alias_in_sql(sql):
    tokens = sql.split()
    result_tokens = []
    i = 0
    while i < len(tokens):
        token = tokens[i]
        if token.upper() == "AS" and i + 1 < len(tokens):
            alias = tokens[i + 1]
            
            # Handle trailing comma attached to alias
            trailing_comma = ''
            if alias.endswith(','):
                alias = alias[:-1]
                trailing_comma = ','
            
            leading = len(alias) - len(alias.lstrip('_'))
            trailing = len(alias) - len(alias.rstrip('_'))
            core = alias.strip('_')

            new_alias = core
            if leading > 0:
                new_alias = "val_" + new_alias
            if trailing > 0:
                new_alias = new_alias + "_val"

            result_tokens.append(token)
            result_tokens.append(new_alias + trailing_comma)
            i += 2
        else:
            result_tokens.append(token)
            i += 1

    # print(result_tokens)
    return " ".join(result_tokens)

def main(session):
    try:
        # Load the configuration from the JSON file
        df = session.sql("""
            SELECT $1
            FROM @RENTALDB.STAGING.RENTAL_STAGE/config.json
            (FILE_FORMAT => jsonconfig_format)
        """)
        rows = df.collect()
        raw_json = rows[0][0]
        config = json.loads(raw_json)
        tables = config['tables']
    except Exception as e:
        return f"Error loading config: {e}"

    try:
        queries = []
        for table in tables:
            column_config = config.get(table['original_name'])
            if not column_config:
                print(f"⚠️ No config found for {table['original_name']}, skipping...")
                continue
            cast_sql = build_cast_sql(table['original_name'], column_config)
            query = f"""
                SELECT {cast_sql}
                FROM RENTALDB.PROD_RENTALMAN_WSDATAIC.{table['original_name']}
                WHERE _FIVETRAN_DELETED = 'FALSE'
            """
            queries.append((query, table))
    except Exception as e:
        return f"Error building queries: {e}"

    try:
        updated_sql_queries = [clean_alias_in_sql(sql[0]) for sql in queries]
        
        # Iterate through each table and generate CTAS
        for (query, table) in zip(updated_sql_queries, tables):
            final_sql = f"""
                CREATE OR REPLACE TABLE RENTALDB.STAGING.{table['full_form_name']} AS {query};
            """
            session.sql(final_sql).collect()
            print(f"✅ Table {table['original_name']} written to STAGING")
        
    except Exception as e:
        return f"Error executing final queries: {e}"

    return "✅ All staging tables processed and renamed successfully."
 
 
$$;
