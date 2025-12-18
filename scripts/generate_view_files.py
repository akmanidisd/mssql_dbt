#!/usr/bin/env python3
"""
Generate Snowflake view script files from metadata.

This script connects to Snowflake, queries the generated view scripts,
and writes them to individual files organized by schema.

Usage:
    python scripts/generate_view_files.py

Requirements:
    - snowflake-connector-python
    - Snowflake credentials in environment variables or ~/.snowflake/config

Environment Variables:
    SNOWFLAKE_ACCOUNT
    SNOWFLAKE_USER
    SNOWFLAKE_PASSWORD
    SNOWFLAKE_WAREHOUSE
    SNOWFLAKE_DATABASE (default: MS_RAW)
    SNOWFLAKE_SCHEMA (default: STG_META)
"""

import os
import sys
from pathlib import Path
from typing import List, Dict, Any

try:
    import snowflake.connector
    from snowflake.connector import DictCursor
except ImportError:
    print("ERROR: snowflake-connector-python not installed")
    print("Install with: pip install snowflake-connector-python")
    sys.exit(1)


def get_snowflake_connection():
    """Create Snowflake connection from environment variables."""
    required_vars = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        print(f"ERROR: Missing required environment variables: {', '.join(missing_vars)}")
        print("\nSet these environment variables:")
        print("  export SNOWFLAKE_ACCOUNT=your_account")
        print("  export SNOWFLAKE_USER=your_user")
        print("  export SNOWFLAKE_PASSWORD=your_password")
        print("  export SNOWFLAKE_WAREHOUSE=your_warehouse  # optional")
        sys.exit(1)

    conn_params = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "database": os.getenv("SNOWFLAKE_DATABASE", "MS_RAW"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA", "STG_META"),
    }

    if warehouse := os.getenv("SNOWFLAKE_WAREHOUSE"):
        conn_params["warehouse"] = warehouse

    try:
        conn = snowflake.connector.connect(**conn_params)
        print(f"✓ Connected to Snowflake: {conn_params['account']}")
        print(f"  Database: {conn_params['database']}")
        print(f"  Schema: {conn_params['schema']}")
        return conn
    except Exception as e:
        print(f"ERROR: Failed to connect to Snowflake: {e}")
        sys.exit(1)


def fetch_view_scripts(conn) -> List[Dict[str, Any]]:
    """Fetch generated view scripts from Snowflake."""
    query = """
        SELECT
            SF_TABLE_SCHEMA,
            SF_TABLE_NAME,
            NEW_TABLE_NAME,
            FILE_PATH,
            DDL_SCRIPT
        FROM MS_RAW.STG_META.V_GENERATED_VIEW_SCRIPTS
        ORDER BY SF_TABLE_SCHEMA, NEW_TABLE_NAME
    """

    try:
        cursor = conn.cursor(DictCursor)
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        print(f"✓ Fetched {len(results)} view scripts from Snowflake")
        return results
    except Exception as e:
        print(f"ERROR: Failed to fetch view scripts: {e}")
        print("\nMake sure you have run these scripts first:")
        print("  1. scripts/staging_meta/11_foundation.sql through 18_fk.sql")
        print("  2. scripts/staging_meta/19_generate_view_scripts.sql")
        sys.exit(1)


def write_view_files(view_scripts: List[Dict[str, Any]], base_path: Path):
    """Write view scripts to individual files organized by schema."""
    stats = {
        "total": 0,
        "by_schema": {}
    }

    for script_data in view_scripts:
        schema = script_data["SF_TABLE_SCHEMA"]
        table_name = script_data["NEW_TABLE_NAME"]
        file_path = script_data["FILE_PATH"]
        ddl_script = script_data["DDL_SCRIPT"]

        # Create full path relative to project root
        full_path = base_path / file_path

        # Create directory if it doesn't exist
        full_path.parent.mkdir(parents=True, exist_ok=True)

        # Write the script
        full_path.write_text(ddl_script, encoding="utf-8")

        # Update statistics
        stats["total"] += 1
        stats["by_schema"][schema] = stats["by_schema"].get(schema, 0) + 1

        print(f"  ✓ {file_path}")

    return stats


def create_combined_scripts(view_scripts: List[Dict[str, Any]], base_path: Path):
    """Create combined deployment scripts per schema."""
    schemas = {}

    # Group scripts by schema
    for script_data in view_scripts:
        schema = script_data["SF_TABLE_SCHEMA"]
        if schema not in schemas:
            schemas[schema] = []
        schemas[schema].append(script_data)

    # Create combined script for each schema
    for schema, scripts in schemas.items():
        combined_script = [
            "-- " + "=" * 70,
            f"-- Combined View Deployment Script for {schema}",
            f"-- Total Views: {len(scripts)}",
            "-- " + "=" * 70,
            "",
            "USE ROLE ACCOUNTADMIN;",
            "",
        ]

        for script_data in scripts:
            combined_script.append(script_data["DDL_SCRIPT"])
            combined_script.append("")

        # Write combined script
        output_path = base_path / "scripts" / "staging_snowflake" / schema / "_DEPLOY_ALL.sql"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text("\n".join(combined_script), encoding="utf-8")

        print(f"  ✓ Combined script: scripts/staging_snowflake/{schema}/_DEPLOY_ALL.sql ({len(scripts)} views)")


def main():
    """Main execution function."""
    print("=" * 70)
    print("Snowflake View Script Generator")
    print("=" * 70)
    print()

    # Get project root (parent of scripts directory)
    project_root = Path(__file__).resolve().parent.parent
    print(f"Project root: {project_root}")
    print()

    # Connect to Snowflake
    conn = get_snowflake_connection()
    print()

    # Fetch view scripts
    view_scripts = fetch_view_scripts(conn)
    print()

    # Write individual view files
    print("Writing individual view scripts...")
    stats = write_view_files(view_scripts, project_root)
    print()

    # Create combined deployment scripts
    print("Creating combined deployment scripts...")
    create_combined_scripts(view_scripts, project_root)
    print()

    # Print summary
    print("=" * 70)
    print("Summary")
    print("=" * 70)
    print(f"Total views generated: {stats['total']}")
    print()
    print("Views by schema:")
    for schema, count in sorted(stats["by_schema"].items()):
        print(f"  {schema}: {count} views")
    print()
    print("✓ All view scripts generated successfully!")
    print()
    print("Next steps:")
    print("  1. Review generated scripts in scripts/staging_snowflake/")
    print("  2. Deploy a single schema: Run scripts/staging_snowflake/<SCHEMA>/_DEPLOY_ALL.sql")
    print("  3. Or deploy individual views as needed")

    conn.close()


if __name__ == "__main__":
    main()
