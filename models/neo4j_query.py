"""Utility to run an arbitrary Cypher query and persist the results as JSON.

Usage example::

<<<<<<< HEAD
    # Test local connection
    python models/neo4j_query.py --test --mode local

    # Test remote connection (requires env vars set)
    python models/neo4j_query.py --test --mode remote

    # Run query on local Neo4j
    python models/neo4j_query.py --mode local \
        --query "MATCH (u:Unit)-[:HAS_CONCEPT]->(c) RETURN u.kode AS kode, collect(c.text) AS concepts" \
        --output data/knowledge_base/query_results.json

    # Run query on remote Neo4j (auto-detects env vars)
    python models/neo4j_query.py \
        --query "MATCH (u:Unit)-[:HAS_CONCEPT]->(c) RETURN u.kode AS kode, collect(c.text) AS concepts" \
        --output data/knowledge_base/query_results.json

    # OR: use environment variables (useful for Neo4j Aura / remote deployments)
    # export NEO4J_URI=neo4j+s://<host>
    # export NEO4J_USERNAME=<user>
    # export NEO4J_PASSWORD=<pass>
    # export NEO4J_DATABASE=<database>
    # export NEO4J_TRUST=all  # (use this to ignore cert verification errors)
    # python models/neo4j_query.py --query "..." --output ...

=======
    python models/neo4j_query.py \
        --uri bolt://localhost:7687 \
        --user neo4j \
        --password secret \
        --query "MATCH (u:Unit)-[:HAS_CONCEPT]->(c) RETURN u.kode AS kode, collect(c.text) AS concepts" \
        --output data/knowledge_base/query_results.json

>>>>>>> 93454fa00cfce8796a4a814231e2ee358eda111b
If the query produces multiple rows, each row becomes an element in the
resulting JSON array.  Value objects are converted using the driver's
`record.data()` method, which returns plain Python dicts and lists.
"""

import argparse
import json
<<<<<<< HEAD
import os
from neo4j import GraphDatabase
from neo4j._conf import TrustAll, TrustCustomCAs, TrustSystemCAs


def load_env_file(path=".env"):
    """Load environment variables from a .env file (if present).

    This is useful when users store Neo4j connection values in a .env file.
    The file is loaded only when those variables are not already set.

    Returns:
        bool: True if a .env file was found and loaded.
    """
    if not os.path.exists(path):
        return False

    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key not in os.environ:
                os.environ[key] = value

    return True


def get_neo4j_config_from_env():
    """Return Neo4j connection configuration from environment variables.

    This supports running the same ingestion script in both local and remote
    environments (e.g., Neo4j Aura) by configuring connection values via
    environment variables.

    Environment variables:
      - NEO4J_URI
      - NEO4J_USERNAME
      - NEO4J_PASSWORD
      - NEO4J_DATABASE
      - NEO4J_TRUST (system|all|custom)
      - NEO4J_TRUST_CERT_PATH (if NEO4J_TRUST=custom)
      - AURA_INSTANCEID
      - AURA_INSTANCENAME

    Returns:
        dict | None: Configuration dict if NEO4J_URI is set, otherwise None.
    """
    uri = os.environ.get("NEO4J_URI")
    if not uri:
        return None

    return {
        "uri": uri,
        "user": os.environ.get("NEO4J_USERNAME", "neo4j"),
        "password": os.environ.get("NEO4J_PASSWORD", ""),
        "database": os.environ.get("NEO4J_DATABASE"),
        "trust": os.environ.get("NEO4J_TRUST", "system").lower(),
        "trust_cert_path": os.environ.get("NEO4J_TRUST_CERT_PATH"),
        "aura_instance_id": os.environ.get("AURA_INSTANCEID"),
        "aura_instance_name": os.environ.get("AURA_INSTANCENAME"),
    }


def _normalize_uri_and_driver_config(uri: str, env_cfg: dict) -> tuple[str, dict]:
    """Normalize URI and build driver kwargs for trust handling.

    Neo4j driver (v5+) requires different handling depending on URI scheme:
      - `neo4j+s://` or `bolt+s://` -> encrypted by default, config settings
        `trusted_certificates`/`encrypted` are NOT allowed.
      - `neo4j+ssc://` or `bolt+ssc://` -> encrypted with self-signed certs.
      - `neo4j://` or `bolt://` -> unencrypted, but can enable encryption via config.

    This helper adjusts the URI (for trust=all) and builds driver kwargs.
    """
    trust = (env_cfg.get("trust") or "system").lower()
    trust_cert_path = env_cfg.get("trust_cert_path")

    # Default: no extra driver kwargs
    driver_kwargs: dict = {}

    # Self-signed / trust-all mode: use +ssc scheme so driver skips cert validation.
    if trust == "all":
        if uri.startswith("neo4j+s://"):
            uri = uri.replace("neo4j+s://", "neo4j+ssc://", 1)
        elif uri.startswith("bolt+s://"):
            uri = uri.replace("bolt+s://", "bolt+ssc://", 1)
        elif uri.startswith("neo4j://"):
            uri = uri.replace("neo4j://", "neo4j+ssc://", 1)
        elif uri.startswith("bolt://"):
            uri = uri.replace("bolt://", "bolt+ssc://", 1)
        return uri, driver_kwargs

    # Custom CA: driver must use normal scheme (bolt/neo4j) and enable encryption + trust.
    if trust == "custom":
        if not trust_cert_path:
            raise ValueError("NEO4J_TRUST=custom requires NEO4J_TRUST_CERT_PATH")

        # Ensure we use a scheme that allows config-based certificate trust.
        if uri.startswith("neo4j+s://") or uri.startswith("neo4j+ssc://"):
            uri = uri.replace("neo4j+s://", "neo4j://", 1).replace("neo4j+ssc://", "neo4j://", 1)
        if uri.startswith("bolt+s://") or uri.startswith("bolt+ssc://"):
            uri = uri.replace("bolt+s://", "bolt://", 1).replace("bolt+ssc://", "bolt://", 1)

        driver_kwargs["encrypted"] = True
        driver_kwargs["trusted_certificates"] = TrustCustomCAs(trust_cert_path)
        return uri, driver_kwargs

    # System trust (default): keep the URI as-is. If uri is plain bolt/neo4j, no encryption.
    # If users want encryption with system CAs on plain scheme, they can explicitly set
    # the URI to neo4j+s:// or bolt+s:// (or use neo4j+s in env).
    #
    # However, if the URI already uses `+s` and the system trust fails (common with
    # Neo4j Aura / self-signed cert chains), auto-fallback to `+ssc` to allow
    # connection without certificate verification.
    if trust == "system":
        if uri.startswith("neo4j+s://"):
            print(
                "⚠️  System trust failed; switching to neo4j+ssc:// to bypass certificate verification. "
                "Set NEO4J_TRUST=system|all if you want to control this behavior."
            )
            uri = uri.replace("neo4j+s://", "neo4j+ssc://", 1)
        elif uri.startswith("bolt+s://"):
            print(
                "⚠️  System trust failed; switching to bolt+ssc:// to bypass certificate verification. "
                "Set NEO4J_TRUST=system|all if you want to control this behavior."
            )
            uri = uri.replace("bolt+s://", "bolt+ssc://", 1)

    return uri, driver_kwargs


def test_connection(driver, session_kwargs):
    """Test Neo4j connection by running a simple query."""
    try:
        with driver.session(**session_kwargs) as session:
            result = session.run("RETURN 'Connection successful' as message")
            record = result.single()
            print(f"✅ Neo4j connection test: {record['message']}")
            return True
    except Exception as e:
        print(f"❌ Neo4j connection test failed: {e}")
        return False


def run_query(driver, session_kwargs, cypher):
    with driver.session(**session_kwargs) as session:
=======
from neo4j import GraphDatabase


def run_query(uri, user, password, cypher):
    driver = GraphDatabase.driver(uri, auth=(user, password))
    with driver.session() as session:
>>>>>>> 93454fa00cfce8796a4a814231e2ee358eda111b
        result = session.run(cypher)
        # each record can be turned into a dict via data()
        return [r.data() for r in result]


def main():
<<<<<<< HEAD
    # Load .env if present so users can keep Neo4j credentials in a file.
    env_loaded = load_env_file()
    if env_loaded:
        print("Loaded .env file")

    parser = argparse.ArgumentParser(description="Run a Cypher query and dump the results.")
    parser.add_argument("--uri", default="bolt://localhost:7687")
    parser.add_argument("--user", default="neo4j")
    parser.add_argument("--password", default=os.environ.get("NEO4J_LOCAL_PASSWORD"))
    parser.add_argument("--query", help="Cypher query to execute (wrap in quotes)")
    parser.add_argument("--output", default="data/knowledge_base/query_results.json", help="path to write JSON output")
    parser.add_argument("--test", action="store_true", help="test connection without running query")
    parser.add_argument("--mode", choices=["local", "remote"], help="force connection mode (local or remote)")
    args = parser.parse_args()

    env_cfg = get_neo4j_config_from_env()
    use_env = False

    if args.mode == "remote":
        if not env_cfg:
            print("❌ Error: --mode remote specified but no environment variables found")
            return
        use_env = True
    elif args.mode == "local":
        use_env = False
    else:
        # Auto-detect: use env if available
        use_env = env_cfg is not None

    if use_env:
        print("Using Neo4j configuration from environment variables (remote)")
        print(f"NEO4J_TRUST mode: {env_cfg.get('trust')}")
        connect_uri = env_cfg["uri"]
        connect_uri, driver_kwargs = _normalize_uri_and_driver_config(connect_uri, env_cfg)
        print(f"Effective URI: {connect_uri}")
        driver = GraphDatabase.driver(
            connect_uri,
            auth=(env_cfg["user"], env_cfg["password"]),
            **driver_kwargs,
        )
        session_kwargs = {k: v for k, v in {"database": env_cfg.get("database")}.items() if v}
    else:
        print("Using local Neo4j configuration")
        connect_uri = args.uri
        driver = GraphDatabase.driver(connect_uri, auth=(args.user, args.password))
        session_kwargs = {}

    print(f"Connecting to: {connect_uri} (database={session_kwargs.get('database', '<default>')})")

    # Test connection if requested
    if args.test:
        success = test_connection(driver, session_kwargs)
        return 0 if success else 1

    # Require query if not testing
    if not args.query:
        print("❌ Error: --query is required when not using --test")
        return 1

    rows = run_query(driver, session_kwargs, args.query)
=======
    parser = argparse.ArgumentParser(description="Run a Cypher query and dump the results.")
    parser.add_argument("--uri", default="bolt://localhost:7687")
    parser.add_argument("--user", default="neo4j")
    parser.add_argument("--password", default="skkni_mentor")
    parser.add_argument("--query", required=True, help="Cypher query to execute (wrap in quotes)")
    parser.add_argument("--output", default="data/knowledge_base/query_results.json", help="path to write JSON output")
    args = parser.parse_args()

    rows = run_query(args.uri, args.user, args.password, args.query)
>>>>>>> 93454fa00cfce8796a4a814231e2ee358eda111b
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    print(f"{len(rows)} row(s) written to {args.output}")


if __name__ == "__main__":
<<<<<<< HEAD
    exit(main())
=======
    main()
>>>>>>> 93454fa00cfce8796a4a814231e2ee358eda111b
