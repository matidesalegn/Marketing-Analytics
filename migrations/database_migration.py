import psycopg2

def migrate_database(source_host, source_port, source_db, source_user, source_password, destination_host, destination_port, destination_db, destination_user, destination_password):
    # Connect to source database
    source_conn = psycopg2.connect(
        host=source_host,
        port=source_port,
        dbname=source_db,
        user=source_user,
        password=source_password
    )

    # Connect to destination database
    destination_conn = psycopg2.connect(
        host=destination_host,
        port=destination_port,
        dbname=destination_db,
        user=destination_user,
        password=destination_password
    )

    # Retrieve list of tables from source database
    with source_conn.cursor() as source_cursor:
        source_cursor.execute("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname='public'")
        tables = source_cursor.fetchall()

        # Migrate each table to destination database
        for table in tables:
            table_name = table[0]
            with source_conn.cursor() as source_cursor:
                source_cursor.execute(f"SELECT * FROM {table_name}")
                rows = source_cursor.fetchall()
                with destination_conn.cursor() as destination_cursor:
                    destination_cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS TABLE {table_name} WITH NO DATA")
                    for row in rows:
                        destination_cursor.execute(f"INSERT INTO {table_name} VALUES %s", (row,))
    
    # Commit changes and close connections
    destination_conn.commit()
    source_conn.close()
    destination_conn.close()

# Example usage
migrate_database(
    source_host='localhost',
    source_port='5432',
    source_db='bank',
    source_user='postgres',
    source_password='Mati@1993',
    destination_host='destination_host',
    destination_port='destination_port',
    destination_db='destination_db',
    destination_user='destination_user',
    destination_password='destination_password'
)
