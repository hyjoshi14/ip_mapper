import logging
import sqlite3
from contextlib import contextmanager
from pathlib import Path

import pandas as pd

QUERIES_FOLDER = Path(".") / "ip_mapper" / "queries"
DB_PATH = Path(".") / "IP_MAPPER.db"

logging.basicConfig(level=logging.INFO)


@contextmanager
def sql_connection():
    """
    Context manager to a SQL cursor.
    """
    conn = sqlite3.connect(str(DB_PATH))
    yield conn
    conn.close()


def load_user_ip_address_data(user_ip_address_cursor):
    """Loads the user_ip_address data to the USER_IP_ADDRESS table."""
    logging.info("Loading user_ip_address mapping file to database.")
    create_table_ddl = QUERIES_FOLDER / "create_user_ip_address_table.sql"
    delete_existing_data_sql = (
        QUERIES_FOLDER / "delete_existing_user_ip_address_data.sql"
    )

    return data_loader(
        data_cursor=user_ip_address_cursor,
        table_name="USER_IP_ADDRESS",
        create_table_ddl=create_table_ddl,
        delete_existing_data_sql=delete_existing_data_sql,
    )


def load_ipv4_data(ipv4_data_cursor, date):
    """Loads the IPV4 data for a particular day to the IPV4_DATA table."""
    logging.info(f"Loading ipv4_data for to database for: {date}.")
    create_table_ddl = QUERIES_FOLDER / "create_ipv4_data_table.sql"
    delete_existing_data_sql = QUERIES_FOLDER / "delete_existing_ipv4_data.sql"

    return data_loader(
        data_cursor=ipv4_data_cursor,
        table_name="IPV4_DATA",
        create_table_ddl=create_table_ddl,
        delete_existing_data_sql=delete_existing_data_sql,
        reported_at=date,
    )


def load_user_country_mapping(date):
    logging.info(f"Loading user_country_mapping to database for: {date}.")
    user_country_mapping = QUERIES_FOLDER / "user_country_mapping.sql"
    user_country_mapping = (
        user_country_mapping.absolute().read_text().format_map({"reported_at": date})
    )

    with sql_connection() as conn:
        df = pd.read_sql(user_country_mapping, con=conn)

    user_country_data_cursor = (row for row in df.T.reset_index().values.T.tolist())
    create_table_ddl = QUERIES_FOLDER / "create_user_country_mapping_table.sql"
    delete_existing_data_sql = (
        QUERIES_FOLDER / "delete_existing_user_country_mapping.sql"
    )

    data_loader(
        data_cursor=user_country_data_cursor,
        table_name="USER_COUNTRY_MAPPING",
        create_table_ddl=create_table_ddl,
        delete_existing_data_sql=delete_existing_data_sql,
        reported_at=date,
    )


def get_user_country_distribution(date):
    """
    Validate the results of the ETL job by listing the Top 10 Countries with the most Users.
    """
    validation_sql = QUERIES_FOLDER / "validation.sql"
    validation_sql = (
        validation_sql.absolute().read_text().format_map({"reported_at": date})
    )

    with sql_connection() as conn:
        df = pd.read_sql(validation_sql, con=conn)

    logging.info(f"Carrying out validation for: {date}")
    print(df.head(20))


def data_loader(
    data_cursor, table_name, create_table_ddl, delete_existing_data_sql, **delete_params
):
    """
    Loads the data for a given cursor to the appropriate table ensuring that
        1. The target table exists
        2. There is no duplication of data

    Args:
        data_cursor (generator): A cursor of the form [[headers],[row1],[row2]]
        table_name (str): Name of the table in which the data is to be inserted
        create_table_ddl (Path): Path to the SQL File that creates the target table
        delete_existing_data_sql (Path): Path to the SQL file that deletes existing data
    Kwargs:
        delete_params: Parameters with which the delete statement is to be executed
    """
    create_table_ddl = create_table_ddl.absolute().read_text()
    delete_existing_data_sql = (
        delete_existing_data_sql.absolute().read_text().format_map(delete_params)
    )

    with sql_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(create_table_ddl)
        cursor.fetchone()
        cursor.execute(delete_existing_data_sql)
        cursor.fetchone()
        cursor.close()

        columns = next(data_cursor)
        data_frame = pd.DataFrame.from_records(data_cursor, columns=columns)
        data_frame.to_sql(table_name, con=conn, if_exists="append", index=False)
