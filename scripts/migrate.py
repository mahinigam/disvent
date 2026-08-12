import os
from pathlib import Path

import clickhouse_connect


def get_client() -> clickhouse_connect.driver.Client:
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        database=os.getenv("CLICKHOUSE_DATABASE", "disvent"),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
    )


def main() -> None:
    client = get_client()

    print("Checking schema_migrations table...")
    client.command(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version String,
            applied_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY version
        """
    )

    result = client.query("SELECT version FROM schema_migrations ORDER BY version")
    applied_versions = {row[0] for row in result.result_rows}

    migrations_dir = Path(__file__).resolve().parents[1] / "migrations"
    migrations = sorted([f for f in migrations_dir.iterdir() if f.name.endswith(".sql")])

    for migration in migrations:
        if migration.name not in applied_versions:
            print(f"Applying migration: {migration.name}")
            sql_content = migration.read_text()
            
            # ClickHouse connect's `command` doesn't support multiple statements separated by ';'
            # We need to split them or run them individually. 
            # For simplicity, we split by ';' ignoring those in strings, but clickhouse-connect supports
            # a `query` or `command` for single statements. Alternatively, `clickhouse_connect` can execute a script.
            # But the best way is to split statements manually or use raw queries.
            
            statements = [stmt.strip() for stmt in sql_content.split(";") if stmt.strip()]
            for stmt in statements:
                try:
                    client.command(stmt)
                except Exception as e:
                    print(f"Failed to execute statement in {migration.name}:")
                    print(stmt)
                    raise e
                    
            client.command(
                f"INSERT INTO schema_migrations (version) VALUES ('{migration.name}')"
            )
            print(f"Successfully applied: {migration.name}")
        else:
            print(f"Skipping already applied migration: {migration.name}")

    print("All migrations applied successfully.")


if __name__ == "__main__":
    main()
