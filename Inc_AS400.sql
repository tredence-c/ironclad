CREATE OR REPLACE PROCEDURE RENTALDB.STAGING.INCREMENTAL_PROCESS_RENTAL_DATA()
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

    return " ".join(result_tokens)


def build_merge_on_clause(keys, target_alias="target", source_alias="source"):
    return " AND ".join([f"{target_alias}.{transform_col(col)} = {source_alias}.{transform_col(col)}" for col in keys])

def transform_col(col):
    if col.startswith('_') and col.endswith('_'):
        # If both start and end with underscore
        return f"val_{col.strip('_')}_val"
    elif col.startswith('_'):
        return f"val_{col.lstrip('_')}"
    elif col.endswith('_'):
        return f"{col.rstrip('_')}_val"
    else:
        return col

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
            """
            queries.append((query, table))
    except Exception as e:
        return f"Error building queries: {e}"

    try:
        updated_sql_queries = [clean_alias_in_sql(sql[0]) for sql in queries]
              
        for (query, table) in zip(updated_sql_queries, tables):
            column_config = config.get(table['original_name'])
            table_name = table['original_name']
            table_full_form_name = table['full_form_name']
            staging_table = f"RENTALDB.STAGING.{table_full_form_name}"
            primary_keys = table.get('primary_keys', [])
            
            if not primary_keys:
                print(f"⚠️ No primary keys defined for table {table_name}, skipping merge...")
                continue
         
            # Step 1: Get MAX _FIVETRAN_SYNCED at ID level (per primary key combination)
            transformed_primary_keys = [transform_col(pk) for pk in primary_keys]
            pk_columns = ', '.join(transformed_primary_keys)
            
            
            max_synced_subquery = f"""
                SELECT 
                    {pk_columns},
                    MAX(val_FIVETRAN_SYNCED) AS MAX_SYNCED
                FROM {staging_table}
                GROUP BY {pk_columns}
            """
            
            print(f"Max synced query per ID: {max_synced_subquery}")
         
            # Step 2: Filter source for incremental data (only rows newer than their ID's max sync)
            incremental_query = f"""
                SELECT src.*
                FROM ({query}) AS src
                LEFT JOIN ({max_synced_subquery}) AS max_sync
                    ON {' AND '.join([f"src.{transform_col(pk)} = max_sync.{transform_col(pk)}" for pk in primary_keys])}
                WHERE src.val_FIVETRAN_SYNCED > COALESCE(max_sync.MAX_SYNCED, TO_TIMESTAMP_TZ('1900-01-01', 'YYYY-MM-DD'))
            """
            
            # Step 3: Build MERGE using composite key
            merge_condition = build_merge_on_clause(primary_keys)
                      
            # ---- Step 4: Build update / insert column lists ----
            transformed_columns = [transform_col(col) for col in column_config.keys()]

            # Exclude PKs
            update_columns = [
                col for col in transformed_columns
                if col not in transformed_primary_keys
            ]

            update_set = ', '.join([f"target.{col} = source.{col}" for col in update_columns])

            insert_cols = ', '.join(transformed_columns)
            insert_vals = ', '.join([f"source.{col}" for col in transformed_columns])
            
            # Step 5: MERGE SQL with proper delete/update/insert logic
            merge_sql = f"""
                MERGE INTO {staging_table} AS target
                USING ({incremental_query}) AS source
                ON {merge_condition}
                
                -- Delete when source is marked as deleted
                WHEN MATCHED AND source.val_FIVETRAN_DELETED = TRUE 
                 AND target.val_fivetran_deleted = FALSE 
                THEN
                    DELETE
                
                -- Update when matched and source is not deleted
                WHEN MATCHED AND source.val_FIVETRAN_DELETED = FALSE THEN 
                    UPDATE SET {update_set}
                
                -- Insert when new record and source is not deleted
                WHEN NOT MATCHED AND source.val_FIVETRAN_DELETED = FALSE THEN 
                    INSERT ({insert_cols})
                    VALUES ({insert_vals});
            """            
            
            session.sql(merge_sql).collect()
            print(f"✅ Incremental MERGE applied to {table_name}")
    
    except Exception as e:
        return f"Error executing final queries: {e}"

    return "✅ All staging tables incremental load completed successfully! "
 
$$;
