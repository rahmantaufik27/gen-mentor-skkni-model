<<<<<<< HEAD
"""Simple Neo4j ingestion helper for knowledge_base_fix.json.
=======
"""Simple Neo4j ingestion helper for knowledge_base.json.
>>>>>>> 93454fa00cfce8796a4a814231e2ee358eda111b

Reads the file produced manually by the user and emits Cypher statements to
create nodes and relations according to the chosen ontology:

    Unit -[:HAS_CONCEPT]-> Concept
    Unit -[:HAS_EVALUATION]-> Evaluation
    Evaluation -[:HAS_QUESTION]-> Question (represented by `soal` property)
    Evaluation -[:HAS_OPTION]-> Option
    Evaluation -[:HAS_CORRECT_ANSWER]-> Answer
    Evaluation -[:HAS_LEVEL]-> Level

The script uses the official ``neo4j`` Python driver.

Usage example:

    # install driver if needed
    pip install neo4j

<<<<<<< HEAD
    # Test local connection
    python models/neo4j_ingest.py --test --mode local

    # Test remote connection (requires env vars set)
    python models/neo4j_ingest.py --test --mode remote

    # Ingest to local Neo4j
    python models/neo4j_ingest.py --mode local --file data/knowledge_base/knowledge_base_fix.json

    # Ingest to remote Neo4j (auto-detects env vars)
    python models/neo4j_ingest.py --file data/knowledge_base/knowledge_base_fix.json

    # OR: use environment variables (useful for Neo4j Aura / remote deployments)
    # export NEO4J_URI=neo4j+s://<host>
    # export NEO4J_USERNAME=<user>
    # export NEO4J_PASSWORD=<pass>
    # export NEO4J_DATABASE=<database>
    # export NEO4J_TRUST=all  # (use this to ignore cert verification errors)
    # python models/neo4j_ingest.py --file data/knowledge_base/knowledge_base_fix.json
=======
    python models/neo4j_ingest.py \
        --uri bolt://localhost:7687 \
        --user neo4j \
        --password secret \
        --file data/knowledge_base/knowledge_base.json
>>>>>>> 93454fa00cfce8796a4a814231e2ee358eda111b

If the database already contains nodes you may want to clear it first. The
script accepts a ``--clear`` flag which will run ``MATCH (n) DETACH DELETE n``
before ingesting.
"""

import json
<<<<<<< HEAD
import os
import argparse
from neo4j import GraphDatabase
from neo4j._conf import TrustAll, TrustCustomCAs, TrustSystemCAs
=======
import argparse
from neo4j import GraphDatabase
>>>>>>> 93454fa00cfce8796a4a814231e2ee358eda111b


def load_kb(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


<<<<<<< HEAD
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


def ingest(tx, kb):

=======
def ingest(tx, kb):
>>>>>>> 93454fa00cfce8796a4a814231e2ee358eda111b
    # iterate through unit list
    for u in kb.get("unit", []):
        kode = u.get("kode_unit") or u.get("kode") or ""
        # create/merge unit node
        tx.run("MERGE (u:Unit {kode: $kode}) SET u.title = $title", kode=kode, title=u.get("judul_unit", ""))
        # concepts
        for concept in u.get("konsep", []):
            tx.run(
                "MERGE (c:Concept {text: $text})\n"
                "MERGE (u:Unit {kode: $kode})\n"
                "MERGE (u)-[:HAS_CONCEPT]->(c)",
                text=concept,
                kode=kode,
            )
        # evaluations
        for eval_obj in u.get("evaluasi", []):
            soal = eval_obj.get("soal", "")
            level = eval_obj.get("bloom_level", "")
            jawaban = eval_obj.get("jawaban", "")
            # create evaluation node (use soal as identifier)
            tx.run(
                "MERGE (e:Evaluation {soal: $soal})\n"
                "SET e.bloom_level = $level, e.jawaban = $jawaban\n"
                "MERGE (u:Unit {kode: $kode})\n"
                "MERGE (u)-[:HAS_EVALUATION]->(e)",
                soal=soal,
                level=level,
                jawaban=jawaban,
                kode=kode,
            )
            # options
            for opt in eval_obj.get("pilihan", []):
                tx.run(
                    "MERGE (o:Option {text: $text})\n"
                    "MERGE (e:Evaluation {soal: $soal})\n"
                    "MERGE (e)-[:HAS_OPTION]->(o)",
                    text=opt,
                    soal=soal,
                )
            # correct answer as separate node
            if jawaban:
                tx.run(
                    "MERGE (a:Answer {text: $text})\n"
                    "MERGE (e:Evaluation {soal: $soal})\n"
                    "MERGE (e)-[:HAS_CORRECT_ANSWER]->(a)",
                    text=jawaban,
                    soal=soal,
                )
            # level node
            if level:
                tx.run(
                    "MERGE (l:Level {name: $level})\n"
                    "MERGE (e:Evaluation {soal: $soal})\n"
                    "MERGE (e)-[:HAS_LEVEL]->(l)",
                    level=level,
                    soal=soal,
                )


def main():
<<<<<<< HEAD
    # Load .env if present so users can keep Neo4j credentials in a file.
    env_loaded = load_env_file()
    if env_loaded:
        print("Loaded .env file")

=======
>>>>>>> 93454fa00cfce8796a4a814231e2ee358eda111b
    parser = argparse.ArgumentParser(description="Ingest knowledge base JSON into Neo4j.")
    parser.add_argument("--uri", default="bolt://localhost:7687")
    parser.add_argument("--user", default="neo4j")
    parser.add_argument("--password", default="skkni_mentor")
    # input knowledge base JSON is now expected under data/knowledge_base
<<<<<<< HEAD
    parser.add_argument("--file", default="../data/knowledge_base/knowledge_base_fix.json")
    parser.add_argument("--clear", action="store_true", help="clear the database before ingest")
    parser.add_argument("--test", action="store_true", help="test connection without ingesting data")
    parser.add_argument("--mode", choices=["local", "remote"], help="force connection mode (local or remote)")
    args = parser.parse_args()

    kb = load_kb(args.file)

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

    with driver.session(**session_kwargs) as session:
        if args.clear:
            session.run("MATCH (n) DETACH DELETE n")
            print("database cleared")
        session.execute_write(ingest, kb)
=======
    parser.add_argument("--file", default="../data/knowledge_base/knowledge_base.json")
    parser.add_argument("--clear", action="store_true", help="clear the database before ingest")
    args = parser.parse_args()

    kb = load_kb(args.file)
    driver = GraphDatabase.driver(args.uri, auth=(args.user, args.password))
    with driver.session() as session:
        if args.clear:
            session.run("MATCH (n) DETACH DELETE n")
            print("database cleared")
        session.write_transaction(ingest, kb)
>>>>>>> 93454fa00cfce8796a4a814231e2ee358eda111b
    print("ingestion complete")


if __name__ == "__main__":
    main()
