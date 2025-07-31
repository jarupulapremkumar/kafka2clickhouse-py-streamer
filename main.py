from clickhouse_connect import get_client
import fastjsonschema
from datetime import datetime
import time,os
import polars as pl
import numpy as np
from confluent_kafka import Producer,Consumer
import orjson
from dotenv import load_dotenv
import logging

def setup_logger():
    """Configures logging for the pipeline."""
    logger = logging.getLogger("Pipeline_logger")
    logger.setLevel(logging.INFO)
    # logger.setLevel(logging.ERROR)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    if not logger.handlers:
        logger.addHandler(handler)

    return logger

# Initialize logger as early as possible
logger = setup_logger()

# 1. Fetch table schema and defaults
def fetch_table_schema(env):
    """
    Fetches the schema of a specified ClickHouse table and returns selected column metadata.

    Args:
        env (dict): A dictionary containing ClickHouse connection parameters:
            - CLICKHOUSE_HOST (str): Hostname of the ClickHouse server.
            - CLICKHOUSE_USER (str): Username for authentication.
            - CLICKHOUSE_PASSWORD (str): Password for authentication.
            - CLICKHOUSE_DATABASE (str): Name of the database.
            - CLICKHOUSE_TABLE (str): Name of the table to describe.

    Returns:
        pandas.DataFrame: A DataFrame containing the columns 'name', 'type', 'default_type', and 'default_expression'
        for each column in the specified ClickHouse table.

    Raises:
        KeyError: If any required key is missing from the env dictionary.
        Exception: If the query or connection fails.
    """
    global logger

    with get_client(
            host=env["CLICKHOUSE_HOST"],
            port=8123,
            username=env["CLICKHOUSE_USER"],
            password=env["CLICKHOUSE_PASSWORD"],
            database=env["CLICKHOUSE_DATABASE"]
        ) as client:

        df = client.query_df(f"DESCRIBE TABLE {env['CLICKHOUSE_TABLE']}")
    return df[['name', 'type', 'default_type', 'default_expression']]

def ch_type_to_json_schema(ch_type: str):
    """
    Converts a ClickHouse data type string to a corresponding JSON Schema type definition.

    Args:
        ch_type (str): The ClickHouse data type as a string.

    Returns:
        dict: A dictionary representing the JSON Schema type for the given ClickHouse type.
            - If the type contains "int", returns {"type": "integer"}.
            - If the type contains "float" or "decimal", returns {"type": "number"}.
            - If the type contains "date" or "datetime", returns {"type": "string"}.
            - If the type contains "string" or "uuid", returns {"type": "string"}.
            - If the type contains "enum", returns {"anyOf": [{"type": "string"}, {"type": "integer"}]}.
            - Otherwise, returns {"type": "string"}.
    """
    global logger

    ch_type = ch_type.lower()
    if "int" in ch_type:
        return {"type": "integer"}
    elif "float" in ch_type or "decimal" in ch_type:
        return {"type": "number"}
    elif "date" in ch_type or "datetime" in ch_type:
        return {"type": "string"}
    elif "string" in ch_type or "uuid" in ch_type:
        return {"type": "string"}
    elif "enum" in ch_type:
        return {"anyOf": [{"type": "string"}, {"type": "integer"}]}
    return {"type": "string"}

def build_json_schema_and_meta(env):
    """
    Builds a JSON schema and metadata mapping for a ClickHouse table schema.

    Args:
        env (dict): Environment configuration dictionary. Must contain any required configuration,
            such as 'REQUIRED_COLUMNS' (list of required column names).

    Returns:
        tuple:
            meta (dict): A mapping from column names to a tuple of (Polars data type, default value).
            schema (dict): A JSON schema dictionary describing the table structure, including
                "type", "properties", and "required" fields.

    Notes:
        - Relies on an external function `fetch_table_schema(env)` to retrieve the table schema as a DataFrame.
        - Uses `ch_type_to_json_schema(dtype)` to convert ClickHouse types to JSON schema types.
        - Handles ClickHouse to Polars type mapping and default values for each column.
        - Columns with unknown types are assigned ("UnknownType", None) in the meta dictionary.
        - The 'db_insert_time' field is removed from meta if present.
    """
    props, required, meta = {}, [], {}

    ch_default_dt_typ = {
        'int_8': -127,
        'int_16': -32767,
        'int_32': -2147483647,
        'int_64': -9223372036854775808,
        'uint_8': 255,
        'uint_16': 65535,
        'uint_32': 4294967295,
        'uint_64': 18446744073709500000,  # Note: too large for Python int or Polars may complain
        'float_32': 2_000_000_000.0,
        'float_64': 999_999_999_999_999.9,
        'utf_8': '',
        'enum': 127,
        'enum_default': 'DEFAULT',
        'int_error_byte_default': -1,
        'date_time': datetime.strptime('1970-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
    }

    # ClickHouse → Polars mapping: CH_TYPE: (PolarsType, default_key_in_dict)
    ch_to_polars_dtype = {
        "Int8": (pl.Int8, "int_8"),
        "Int16": (pl.Int16, "int_16"),
        "Int32": (pl.Int32, "int_32"),
        "Int64": (pl.Int64, "int_64"),
        "UInt8": (pl.UInt8, "uint_8"),
        "UInt16": (pl.UInt16, "uint_16"),
        "UInt32": (pl.UInt32, "uint_32"),
        "UInt64": (pl.UInt64, "uint_64"),
        "Float32": (pl.Float32, "float_32"),
        "Float64": (pl.Float64, "float_64"),
        "Date": (pl.Date, "date_time"),
        "DateTime": (pl.Datetime, "date_time"),
        # Extend if needed
    }

    schema_df = fetch_table_schema(env)  # You must define this function externally

    for name, dtype, *_ in schema_df.itertuples(index=False): 
        props[name] = ch_type_to_json_schema(dtype)

        if "Enum" in dtype:
            meta[name] = (pl.UInt8, ch_default_dt_typ["enum"])
        elif "String" in dtype or "UUID" in dtype:
            meta[name] = (pl.Utf8, ch_default_dt_typ["utf_8"])
        elif dtype in ch_to_polars_dtype:
            polars_type, default_key = ch_to_polars_dtype[dtype]
            meta[name] = (polars_type, ch_default_dt_typ[default_key])
        else:
            print(f"Unknown ClickHouse type: {dtype} for column {name}")
            meta[name] = ("UnknownType", None)  # You may want to raise or log here

    required = env.get('REQUIRED_COLUMNS', [])
    meta.pop('db_insert_time', None)  # Optional field

    return meta, {
        "type": "object",
        "properties": props,
        "required": required,
       # "additionalProperties": False
    }

# --- Schema loader ---
def build_and_compile_schema(env):
    """
    Builds a JSON schema and its metadata for the given environment, then compiles the schema.

    Args:
        env: The environment or configuration context used to build the schema.

    Returns:
        tuple: A tuple containing:
            - meta: Metadata associated with the generated schema.
            - compiled: A compiled fastjsonschema validator function.

    Logs:
        Logs the schema building process using the global logger.
    """
    global logger
    logger.info(" Building schema...")
    meta, schema = build_json_schema_and_meta(env)
    compiled = fastjsonschema.compile(schema)
    return meta, compiled

# 2. Compile JSON Schema
def validate_json_rows(data, schema_fn):
    """
    Validates a list of JSON-like data rows against a provided schema validation function.

    Args:
        data (list): A list of dictionaries (rows) to be validated.
        schema_fn (callable): A function that takes a row as input and raises an exception if the row is invalid.

    Returns:
        tuple: A tuple containing two lists:
            - valid (list): Rows that passed validation.
            - invalid (list): Rows that failed validation, each represented as a dictionary with keys:
                - 'model': The value of env['VARIANT'] as a string.
                - 'row': The invalid row.
                - 'error': The exception message as a string.

    Notes:
        - Uses a global logger for logging (if implemented elsewhere).
        - Assumes 'env' is a global variable containing the 'VARIANT' key.
    """
    global logger
    valid, invalid = [], []
    for row in data:
        try:
            schema_fn(row)
            valid.append(row)
        except Exception as e:
            invalid.append({'row': row,'error': str(e)})
    return valid, invalid

def to_polars_df(rows, cols_meta, datetime_cols, string_enum_cols):
    """
    This function ensures that all columns specified in `cols_meta` are present in the resulting DataFrame,
    casting them to the appropriate types, filling missing columns with default values, and handling nulls
    with sensible defaults. Special handling is provided for datetime columns and string-enum columns.
    Args:
        rows (list[dict]): List of dictionaries representing the data rows.
        cols_meta (dict): Dictionary mapping column names to a tuple of (target_type, default_value).
        datetime_cols (set or list): Set or list of column names (lowercase) that should be parsed as datetimes.
        string_enum_cols (set or list): Set or list of column names (lowercase) that should be treated as string enums.
    Returns:
        pl.DataFrame: A Polars DataFrame with columns cast to the specified types, missing columns filled with defaults,
                      and nulls handled according to column type.
    Notes:
        - Datetime columns are parsed from strings, with empty or null values filled with 1970-01-01.
        - String-enum columns are cast to Utf8, with nulls filled as "DEFAULT".
        - Other columns are cast to their target type, with nulls filled with the provided default value.
        - Columns missing from the input are created with the default value and cast to the target type.
    
    Transforms a list of dictionaries into a correctly-typed Polars DataFrame.
    Handles missing columns, fills nulls with defaults, casts types with fallback logic.
    """
    global logger
    
    if not rows:
        return pl.DataFrame()

    source_df = pl.DataFrame(rows)
    expressions = []

    for col, v in cols_meta.items():
        target_type = v[0]
        default_value = v[1]

        if col in source_df.columns:
            col_expr = pl.col(col)

            if col.lower() in datetime_cols:
                expr = (
                    pl.when(col_expr.cast(pl.Utf8) == '')
                    .then(pl.lit(datetime(1970, 1, 1)))
                    .otherwise(col_expr.cast(pl.Utf8).str.strptime(pl.Datetime, strict=False))
                    .fill_null(datetime(1970, 1, 1))
                )

            elif col.lower() in string_enum_cols:
                expr = (
                    pl.when(col_expr.is_null())
                    .then(pl.lit("DEFAULT"))
                    .otherwise(col_expr)
                    .cast(pl.Utf8, strict=False)
                )

            else:
                expr = pl.when(pl.col(col).is_null()).then(pl.lit(default_value)).otherwise(pl.col(col)).cast(target_type, strict=False)

                # # First try to cast to expected type (strict=False)
                # expr_try = col_expr.cast(target_type, strict=False)

                # # Fallback: if cast fails (result is null), cast to Utf8
                # expr = (
                #     pl.when(expr_try.is_null())
                #     .then(col_expr.cast(pl.Utf8, strict=False))
                #     .otherwise(expr_try)
                # ).fill_null(pl.lit(default_value))

        else:
            # If column is missing, fill with default value and cast
            expr = pl.lit(default_value).cast(target_type)

        expressions.append(expr.alias(col))

    return source_df.select(expressions)

# 4. Insert
def insert_valid_rows(valid_data,cols_meta,env):
    """
    Inserts valid rows into a ClickHouse table using Arrow format with asynchronous insert settings.
    Args:
        valid_data (list or DataFrame): The validated data rows to be inserted.
        cols_meta (dict): Metadata describing the columns, used for DataFrame conversion.
        env (dict): Environment configuration containing ClickHouse connection details and column type information.
            Required keys:
                - 'DATETIME_COLUMNS': List of column names to treat as datetime.
                - 'STRING_ENUM_COLUMNS': List of column names to treat as string enums.
                - 'CLICKHOUSE_HOST': ClickHouse server host.
                - 'CLICKHOUSE_USER': ClickHouse username.
                - 'CLICKHOUSE_PASSWORD': ClickHouse password.
                - 'CLICKHOUSE_DATABASE': Target ClickHouse database.
                - 'CLICKHOUSE_TABLE': Target ClickHouse table.
    Returns:
        None
    Logs:
        - Info message if there are no valid rows to insert.
        - Info message after successful insertion indicating the number of rows inserted.
    Raises:
        Any exceptions raised by the ClickHouse client or data conversion utilities.
    """
    global logger
    settings = {
        'async_insert': 1,                          # Enables async insert
        'wait_for_async_insert': 1,                 # Don’t wait; returns immediately set 0 else wait till insert happens 1
        'async_insert_threads': 4,                  # Use more threads (depending on CPU cores)
        'async_insert_busy_timeout_ms': 500,        # Shorter timeout to retry
        'async_insert_max_data_size': 100_000_000,  # Reasonable batch size limit (100 MB)
    }

    if not valid_data:
        logger.info("No valid rows to insert")
        return
    
    arrow_table = to_polars_df(valid_data, cols_meta,env['DATETIME_COLUMNS'],env['STRING_ENUM_COLUMNS']).to_arrow()
    
    with get_client(
            host=env["CLICKHOUSE_HOST"],
            port=8123,
            username=env["CLICKHOUSE_USER"],
            password=env["CLICKHOUSE_PASSWORD"],
            database=env["CLICKHOUSE_DATABASE"]
        ) as client:
    
        client.insert_arrow(
            table=env['CLICKHOUSE_TABLE'],
            arrow_table=arrow_table,
            settings=settings  # Use async insert settings
        )
        logger.info(f"Inserted {arrow_table.num_rows} rows into {env['CLICKHOUSE_TABLE']}")

def push_raw_to_dlq(records,env):
    """
    Push raw JSON records (no key, no error reason) to DLQ.
    """
    global logger
    # DLQ Kafka config
    dlq_config = {
        'bootstrap.servers': env['KAFKA_BROKER'],  # Update with your Kafka broker
        'client.id': env['KAFKA_PRODUCER_CLIENT_ID'],
        'linger.ms': 5
    }

    # Initialize producer
    dlq_producer = Producer(dlq_config)

    for record in records:
        try:
            dlq_producer.produce(
                topic=env['KAFKA_DLQ_TOPIC'],  # Update with your DLQ topic
                value=orjson.dumps(record)
            )
            logger.info(f"Pushed to DLQ: {record}")
        except Exception as e:
            logger.error(f"Failed to push to DLQ: {e} -- Record: {record}")
    

    dlq_producer.flush()

def post_commit_callback(err, ps):
    """Asynchronously gets commit responses from Kafka."""
    global logger
    if err:
        logger.error(f"Commit error: {err}")
    else:
        logger.info("Offsets committed successfully!")

def process_packets(packets,env):
    """
    Processes a batch of packets by validating them against a compiled schema, inserting valid rows, and handling invalid or failed inserts.

    Args:
        packets (list): List of packet data (typically dictionaries) to be processed.
        env (dict): Environment configuration dictionary, expected to contain at least the 'VARIANT' key.

    Workflow:
        1. Builds and compiles the schema if not already cached.
        2. Validates packets against the compiled schema, separating valid and invalid packets.
        3. Attempts to insert valid packets into the target storage.
        4. On insert failure, rebuilds the schema, re-validates all packets, and retries the insert.
        5. If the retry fails, pushes the failed valid packets to the Dead Letter Queue (DLQ).
        6. Sends all invalid packets to the DLQ.
        7. Logs the number of valid and invalid packets, as well as processing time.

    Exceptions:
        - Handles and logs exceptions during insert operations.
        - Ensures that failed or invalid packets are pushed to the DLQ for further inspection.

    Side Effects:
        - May update global variables: cols_meta, compiled_schema, logger.
        - Sends records to a Dead Letter Queue (DLQ) on failure.
        - Logs processing steps and errors.
    """
    start = time.time()
    global cols_meta, compiled_schema,logger
    # Fetch schema if not cached
    if not cols_meta or not compiled_schema:
        cols_meta, compiled_schema = build_and_compile_schema(env)
        logger.info("Schema built and compiled successfully.")

    #Validate packets
    valid, invalid = validate_json_rows(packets, compiled_schema)
    logger.info(f"Valid: {len(valid)}, Invalid: {len(invalid)}")

    try:
        insert_valid_rows(valid, cols_meta,env)
    except Exception as e:
        logger.error(f" Insert failed: {e}\n... Rebuilding schema and retrying...")

        # Rebuild schema
        cols_meta, compiled_schema = build_and_compile_schema(env)

        # Re-validate all packets
        valid, invalid = validate_json_rows(packets, compiled_schema)
        logger.error(f"Retry - Valid: {len(valid)},  Invalid: {len(invalid)}")

        # Retry insert valid
        try:
            insert_valid_rows(valid, cols_meta,env)
        except Exception as final_e:
            logger.error(f" Final insert failed: {final_e}.\n Pushing to DLQ")
            if len(valid) > 0:
                # Send invalid to DLQ
                push_raw_to_dlq([{'row': row,'error': str(final_e)} for row in valid],env)

        # Send invalid to DLQ
        if invalid:
            push_raw_to_dlq(invalid,env)
            logger.info(f"Sent {len(invalid)} records to DLQ")
            invalid = []  # Clear invalid after sending to DLQ


    # Send invalid to DLQ
    if invalid:
        push_raw_to_dlq(invalid,env)
        logger.info(f" Sent {len(invalid)} records to DLQ")
        invalid = []  # Clear invalid after sending to DLQ


    logger.info(f" Processed {len(packets)} packets in {time.time() - start:.2f} sec")

def main(env):
    """
    Main entry point for the Kafka consumer service.
    This function initializes a Kafka consumer using the provided environment configuration,
    subscribes to the specified Kafka topic, and continuously polls for messages. It decodes
    the received messages, filters them based on the specified model variant, and processes
    the selected packets. Offsets are committed manually after processing. The function
    handles graceful shutdown on keyboard interruption and logs relevant events.
    Args:
        env (dict): A dictionary containing environment variables and configuration values
            required for Kafka consumer setup and message processing. Expected keys include:
            - "KAFKA_BROKER": Kafka broker address.
            - "KAFKA_CONSUMER_GROUP_ID": Consumer group ID.
            - "KAFKA_AUTO_OFFSET_RESET": Offset reset policy.
            - "KAFKA_SOURCE_TOPIC": Kafka topic to subscribe to.
            - "NUM_MESSAGES": Number of messages to consume per poll.
            - "VARIANT": Model variant to filter messages.
    Raises:
        Exception: Logs and handles any unexpected exceptions during consumer operation.
    """
    
    global logger

    # Kafka consumer configuration
    consumer_config = {
        'bootstrap.servers': env["KAFKA_BROKER"],
        'group.id': env["KAFKA_CONSUMER_GROUP_ID"],
        'auto.offset.reset': env["KAFKA_AUTO_OFFSET_RESET"],
        'fetch.max.bytes': 50 * 1024 * 1024,  # Increase if needed
        #'on_commit': post_commit_callback,
    }

    # Create the Kafka consumer
    consumer = Consumer(consumer_config)
    logger.info("Kafka consumer connected.")
    consumer.subscribe([env["KAFKA_SOURCE_TOPIC"]])
    logger.info("Kafka consumer subscribed to topic.")

    try:
        while True:
            # Poll messages
            messages = consumer.consume(num_messages=int(env['NUM_MESSAGES']), timeout=1.0)

            if not messages:
                continue
            
            # Decode messages using list comprehension
            # packets = [orjson.loads(msg.value().decode('utf-8')) for msg in messages if not msg.error()]
            packets = [
                orjson.loads(msg.value().decode('utf-8')) 
                for msg in messages 
                if not msg.error() and msg.value() and msg.value().strip()
            ]

            if packets:
                process_packets(packets,env)
                # Commit offsets manually
            if len(packets) > 0:
                consumer.commit(asynchronous=True)

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user.")
    except Exception as err:
        logger.exception(f"Unexpected error: {err}")
    finally:
        consumer.close()
        logger.info("Kafka consumer closed.")

# 🎯 Run pipeline
if __name__ == '__main__':
    cols_meta = {}
    compiled_schema = None

    # Read environment variables
    env = {
            "KAFKA_BROKER": os.environ.get("KAFKA_BROKER"),
            "KAFKA_CONSUMER_GROUP_ID": os.environ.get("KAFKA_CONSUMER_GROUP_ID"),
            "KAFKA_PRODUCER_CLIENT_ID": os.environ.get("KAFKA_PRODUCER_CLIENT_ID"),
            "KAFKA_SOURCE_TOPIC": os.environ.get("KAFKA_SOURCE_TOPIC"),
            "KAFKA_DLQ_TOPIC": os.environ.get("KAFKA_DLQ_TOPIC"),
            "KAFKA_AUTO_OFFSET_RESET": os.environ.get("KAFKA_AUTO_OFFSET_RESET"),
            "NUM_MESSAGES" : os.environ.get("NUM_MESSAGES"),
            "CLICKHOUSE_HOST": os.environ.get("CLICKHOUSE_HOST"),
            "CLICKHOUSE_DATABASE": os.environ.get("CLICKHOUSE_DATABASE"),
            "CLICKHOUSE_USER": os.environ.get("CLICKHOUSE_USER"),
            "CLICKHOUSE_PASSWORD": os.environ.get("CLICKHOUSE_PASSWORD"),
            "CLICKHOUSE_TABLE": os.environ.get("CLICKHOUSE_TABLE"),
            "REQUIRED_COLUMNS": (os.environ.get("REQUIRED_COLUMNS") or "").split(','),
            "DATETIME_COLUMNS": (os.environ.get("DATETIME_COLUMNS") or "").split(','),
            "STRING_ENUM_COLUMNS": (os.environ.get("STRING_ENUM_COLUMNS") or "gps_validity,incognito_mode").split(','),
        }
    # Load environment variables from .env file if it exists
    if env['KAFKA_BROKER'] is None:
        load_dotenv()
        env = {
            "KAFKA_BROKER": os.getenv("KAFKA_BROKER"),
            "KAFKA_CONSUMER_GROUP_ID": os.getenv("KAFKA_CONSUMER_GROUP_ID"),
            "KAFKA_PRODUCER_CLIENT_ID": os.getenv("KAFKA_PRODUCER_CLIENT_ID"),
            "KAFKA_SOURCE_TOPIC": os.getenv("KAFKA_SOURCE_TOPIC"),
            "KAFKA_DLQ_TOPIC": os.getenv("KAFKA_DLQ_TOPIC"),
            "KAFKA_AUTO_OFFSET_RESET": os.getenv("KAFKA_AUTO_OFFSET_RESET"),
            "NUM_MESSAGES" : os.getenv("NUM_MESSAGES") or 25000,
            "CLICKHOUSE_HOST": os.getenv("CLICKHOUSE_HOST"),
            "CLICKHOUSE_DATABASE": os.getenv("CLICKHOUSE_DATABASE"),
            "CLICKHOUSE_USER": os.getenv("CLICKHOUSE_USER"),
            "CLICKHOUSE_PASSWORD": os.getenv("CLICKHOUSE_PASSWORD"),
            "CLICKHOUSE_TABLE": os.getenv("CLICKHOUSE_TABLE"),
            "REQUIRED_COLUMNS": (os.getenv("REQUIRED_COLUMNS") or "").split(','),
            "DATETIME_COLUMNS": (os.getenv("DATETIME_COLUMNS") or "").split(','),
            "STRING_ENUM_COLUMNS": (os.getenv("STRING_ENUM_COLUMNS") or "gps_validity,incognito_mode").split(','),
        }

    if not all(env.values()):
        raise ValueError("Missing required environment variables")
    else:
        main(env)



