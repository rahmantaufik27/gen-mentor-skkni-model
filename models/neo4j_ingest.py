"""Simple Neo4j ingestion helper for knowledge_base_fix.json.

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

If the database already contains nodes you may want to clear it first. The
script accepts a ``--clear`` flag which will run ``MATCH (n) DETACH DELETE n``
before ingesting.
"""

import json
import os
import argparse
from typing import Dict, Optional

from neo4j import GraphDatabase
from neo4j._conf import TrustAll, TrustCustomCAs, TrustSystemCAs


def load_kb(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


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


def _normalize_section_name(comment_line: str) -> str:
    """Convert a section comment into a normalized section key."""
    # Example: "# INSTANCE KB RAW CONFIGURATION" -> "kb_raw"
    s = comment_line.lstrip("#").strip().lower()

    # Strip the leading 'instance' if present
    if s.startswith("instance"):
        s = s[len("instance") :].strip()
    # Strip any trailing 'configuration' if present
    if s.endswith("configuration"):
        s = s[: -len("configuration")].strip()

    # Normalize to safe identifier (e.g. spaces -> underscores)
    import re

    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return s or "default"


def load_env_sections(path=".env"):
    """Load a .env file into named sections.

    Sections are detected by comment lines that include the word "INSTANCE".
    Example:

        # INSTANCE KB RAW CONFIGURATION
        NEO4J_URI=...

        # INSTANCE GENERATED QUESTION BANK CONFIGURATION
        NEO4J_URI=...

    Returns:
        dict: {section_name: {key: value, ...}}
    """

    sections: Dict[str, Dict[str, str]] = {"default": {}}
    if not os.path.exists(path):
        return sections

    current = "default"
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if line.startswith("#"):
                if "INSTANCE" in line.upper():
                    current = _normalize_section_name(line)
                    sections.setdefault(current, {})
                continue
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            sections.setdefault(current, {})[key] = value

    return sections


def get_neo4j_config_from_env_section(section: Optional[str] = None):
    """Get Neo4j config either from a named section in .env or from environment."""

    sections = load_env_sections()

    if section:
        # try exact match, then lowercase
        cfg = sections.get(section) or sections.get(section.lower())
        if cfg:
            uri = cfg.get("NEO4J_URI") or os.environ.get("NEO4J_URI")
            if not uri:
                return None
            return {
                "uri": uri,
                "user": cfg.get("NEO4J_USERNAME") or os.environ.get("NEO4J_USERNAME", "neo4j"),
                "password": cfg.get("NEO4J_PASSWORD") or os.environ.get("NEO4J_PASSWORD", ""),
                "database": cfg.get("NEO4J_DATABASE") or os.environ.get("NEO4J_DATABASE"),
                "trust": (cfg.get("NEO4J_TRUST") or os.environ.get("NEO4J_TRUST", "system")).lower(),
                "trust_cert_path": cfg.get("NEO4J_TRUST_CERT_PATH") or os.environ.get("NEO4J_TRUST_CERT_PATH"),
                "aura_instance_id": cfg.get("AURA_INSTANCEID") or os.environ.get("AURA_INSTANCEID"),
                "aura_instance_name": cfg.get("AURA_INSTANCENAME") or os.environ.get("AURA_INSTANCENAME"),
            }

    # Fallback to classic environment reading
    return get_neo4j_config_from_env()


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
    """Ingest normalized knowledge base (unit-oriented) into Neo4j.

    This is used by the legacy/"fix" data path where data is already shaped as
    a list of units.
    """

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


def ingest_raw_kb(tx, raw):
    """Ingest a raw knowledge base JSON (role/skema/unit) into Neo4j.

    Schema:
      Role -[:HAS_SCHEMA]-> Schema
      Schema -[:HAS_UNIT]-> Unit
      Unit -[:HAS_CONCEPT]-> Concept
      Unit -[:HAS_EVALUATION]-> Evaluation
      Evaluation -[:HAS_OPTION]-> Option
      Evaluation -[:HAS_CORRECT_ANSWER]-> Answer
      Evaluation -[:HAS_LEVEL]-> Level
    """

    role = raw.get("role") or ""
    tx.run("MERGE (r:Role {name: $role})", role=role)

    for skema in raw.get("skema", []):
        schema_name = skema.get("nama_skema") or ""
        tx.run("MERGE (s:Schema {name: $schema_name})", schema_name=schema_name)
        tx.run(
            "MERGE (r:Role {name: $role})\n"
            "MERGE (s:Schema {name: $schema_name})\n"
            "MERGE (r)-[:HAS_SCHEMA]->(s)",
            role=role,
            schema_name=schema_name,
        )

        for unit in skema.get("unit_kompetensi", []):
            kode = unit.get("kode_unit") or unit.get("kode") or ""
            title = unit.get("judul_unit", "")
            tx.run("MERGE (u:Unit {kode: $kode}) SET u.title = $title", kode=kode, title=title)
            tx.run(
                "MERGE (s:Schema {name: $schema_name})\n"
                "MERGE (u:Unit {kode: $kode})\n"
                "MERGE (s)-[:HAS_UNIT]->(u)",
                schema_name=schema_name,
                kode=kode,
            )

            for concept in unit.get("konsep", []):
                tx.run(
                    "MERGE (c:Concept {text: $text})\n"
                    "MERGE (u:Unit {kode: $kode})\n"
                    "MERGE (u)-[:HAS_CONCEPT]->(c)",
                    text=concept,
                    kode=kode,
                )

            for eval_obj in unit.get("evaluasi", []):
                soal = eval_obj.get("soal", "")
                level = eval_obj.get("bloom_level", "")
                jawaban = eval_obj.get("jawaban", "")

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

                for opt in eval_obj.get("pilihan", []):
                    tx.run(
                        "MERGE (o:Option {text: $text})\n"
                        "MERGE (e:Evaluation {soal: $soal})\n"
                        "MERGE (e)-[:HAS_OPTION]->(o)",
                        text=opt,
                        soal=soal,
                    )

                if jawaban:
                    tx.run(
                        "MERGE (a:Answer {text: $text})\n"
                        "MERGE (e:Evaluation {soal: $soal})\n"
                        "MERGE (e)-[:HAS_CORRECT_ANSWER]->(a)",
                        text=jawaban,
                        soal=soal,
                    )

                if level:
                    tx.run(
                        "MERGE (l:Level {name: $level})\n"
                        "MERGE (e:Evaluation {soal: $soal})\n"
                        "MERGE (e)-[:HAS_LEVEL]->(l)",
                        level=level,
                        soal=soal,
                    )


def ingest_generated_questions(tx, data):
    """Ingest generated questions from generated_question_all.json.

    Assumes structure:
    {
      "role": "...",
      "skema": [
        {
          "nama_skema": "...",
          "unit_kompetensi": [
            {
              "kode_unit": "...",
              "bloom_level": "...",
              "soal": "...",
              "pilihan": [...],
              "jawaban": "..."
            },
            ...
          ]
        }
      ]
    }

    Schema:
      Role -[:HAS_SCHEMA]-> Schema
      Schema -[:HAS_UNIT]-> Unit
      Unit -[:HAS_QUESTION]-> Soal
      Soal -[:HAS_OPTION]-> Pilihan
      Soal -[:HAS_CORRECT_ANSWER]-> Jawaban
      Soal -[:HAS_LEVEL]-> Bloom_level
    """

    role = data.get("role") or "generated_questions"
    tx.run("MERGE (r:Role {name: $role})", role=role)

    for skema in data.get("skema", []):
        schema_name = skema.get("nama_skema") or "question_bank"
        tx.run("MERGE (s:Schema {name: $schema_name})", schema_name=schema_name)
        tx.run(
            "MERGE (r:Role {name: $role})\n"
            "MERGE (s:Schema {name: $schema_name})\n"
            "MERGE (r)-[:HAS_SCHEMA]->(s)",
            role=role,
            schema_name=schema_name,
        )

        for unit in skema.get("unit_kompetensi", []):
            kode = unit.get("kode_unit") or ""
            tx.run("MERGE (u:Unit {kode: $kode})", kode=kode)
            tx.run(
                "MERGE (s:Schema {name: $schema_name})\n"
                "MERGE (u:Unit {kode: $kode})\n"
                "MERGE (s)-[:HAS_UNIT]->(u)",
                schema_name=schema_name,
                kode=kode,
            )

            # Create Soal node and link it to Unit
            soal_text = unit.get("soal", "")
            if soal_text:
                tx.run(
                    "MERGE (q:Soal {text: $soal})\n"
                    "MERGE (u:Unit {kode: $kode})\n"
                    "MERGE (u)-[:HAS_QUESTION]->(q)",
                    soal=soal_text,
                    kode=kode,
                )

                # Create Pilihan nodes linked to the Soal
                for pilihan in unit.get("pilihan", []):
                    tx.run(
                        "MERGE (p:Pilihan {text: $pilihan})\n"
                        "MERGE (q:Soal {text: $soal})\n"
                        "MERGE (q)-[:HAS_OPTION]->(p)",
                        pilihan=pilihan,
                        soal=soal_text,
                    )

                # Create Jawaban node linked to the Soal
                jawaban_text = unit.get("jawaban", "")
                if jawaban_text:
                    tx.run(
                        "MERGE (j:Jawaban {text: $jawaban})\n"
                        "MERGE (q:Soal {text: $soal})\n"
                        "MERGE (q)-[:HAS_CORRECT_ANSWER]->(j)",
                        jawaban=jawaban_text,
                        soal=soal_text,
                    )

                # Create Bloom_level node linked to the Soal
                level = unit.get("bloom_level", "")
                if level:
                    tx.run(
                        "MERGE (b:Bloom_level {name: $level})\n"
                        "MERGE (q:Soal {text: $soal})\n"
                        "MERGE (q)-[:HAS_LEVEL]->(b)",
                        level=level,
                        soal=soal_text,
                    )


def normalize_kb_raw(raw):
    """Normalize the raw knowledge base JSON into the expected ingestion shape.

    The `data/knowledge_base/knowledge_base_raw.json` file uses a hierarchy:
    ``skema -> unit_kompetensi -> ...``. The ingestion code expects a top-level
    "unit" list.
    """

    units = []
    for skema in raw.get("skema", []):
        for unit in skema.get("unit_kompetensi", []):
            units.append(unit)

    return {"unit": units}


def _connect_and_ingest(cfg, ingest_fn, payload, clear=True):
    """Connect to Neo4j with the given config and write the payload."""

    connect_uri = cfg["uri"]
    connect_uri, driver_kwargs = _normalize_uri_and_driver_config(connect_uri, cfg)

    driver = GraphDatabase.driver(
        connect_uri,
        auth=(cfg["user"], cfg["password"]),
        **driver_kwargs,
    )

    session_kwargs = {k: v for k, v in {"database": cfg.get("database")}.items() if v}
    print(f"Connecting to: {connect_uri} (database={session_kwargs.get('database', '<default>')})")

    # Validate connection before writing/clearing data.
    if not test_connection(driver, session_kwargs):
        raise RuntimeError("Unable to connect to Neo4j; aborting ingestion")

    with driver.session(**session_kwargs) as session:
        if clear:
            session.run("MATCH (n) DETACH DELETE n")
            print("database cleared")
        session.execute_write(ingest_fn, payload)


def ingest_kb_raw_aura(file_path="data/knowledge_base/knowledge_base_raw.json", section="kb_raw"):
    """Ingest `knowledge_base_raw.json` into Neo4j Aura.

    This function reads the config for the given `section` from the .env file,
    clears the remote database, and ingests using the role/schema/unit hierarchy.
    """

    kb_raw = load_kb(file_path)

    cfg = get_neo4j_config_from_env_section(section)
    if not cfg:
        raise RuntimeError(f"No Neo4j config found for section '{section}'")

    _connect_and_ingest(cfg, ingest_raw_kb, kb_raw, clear=True)


def ingest_generated_questions_aura(
    file_path="data/generated_question/generated_question_all.json",
    section="generated_question_bank",
):
    """Ingest `generated_question_all.json` into Neo4j Aura.

    This function reads the config for the given `section` from the .env file,
    clears the remote database, and ingests generated questions.
    """

    questions = load_kb(file_path)

    cfg = get_neo4j_config_from_env_section(section)
    if not cfg:
        raise RuntimeError(f"No Neo4j config found for section '{section}'")

    _connect_and_ingest(cfg, ingest_generated_questions, questions, clear=True)


def main():
    # Load .env if present so users can keep Neo4j credentials in a file.
    env_loaded = load_env_file()
    if env_loaded:
        print("Loaded .env file")

    parser = argparse.ArgumentParser(description="Ingest JSON into Neo4j.")
    parser.add_argument("--uri", default="bolt://localhost:7687")
    parser.add_argument("--user", default="neo4j")
    parser.add_argument("--password", default="skkni_mentor")
    parser.add_argument(
        "--source",
        choices=["fix", "raw", "generated"],
        default="fix",
        help=(
            "Source data type: 'fix' uses knowledge_base_fix.json, "
            "'raw' uses knowledge_base_raw.json, 'generated' uses generated_question_all.json."
        ),
    )
    parser.add_argument(
        "--file",
        help="Path to input JSON file (optional; defaults depend on --source).",
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Clear the database before ingest (only applies to local mode).",
    )
    parser.add_argument("--test", action="store_true", help="Test connection without ingesting data")
    parser.add_argument("--mode", choices=["local", "remote"], help="Force connection mode (local or remote)")
    parser.add_argument(
        "--section",
        help=(
            "When using remote mode, choose which .env INSTANCE section to read. "
            "E.g. 'kb_raw' or 'generated_question_bank'."
        ),
    )
    args = parser.parse_args()

    # Determine input file and ingestion behavior based on source type
    if args.source == "raw":
        input_file = args.file or "data/knowledge_base/knowledge_base_raw.json"
        kb = load_kb(input_file)
        ingest_fn = ingest_raw_kb
        default_section = "kb_raw"
    elif args.source == "generated":
        input_file = args.file or "data/generated_question/generated_question_all.json"
        kb = load_kb(input_file)
        ingest_fn = ingest_generated_questions
        default_section = "generated_question_bank"
    else:
        input_file = args.file or "data/knowledge_base/knowledge_base_fix.json"
        kb = load_kb(input_file)
        ingest_fn = ingest
        default_section = None

    env_cfg = None
    if args.mode == "remote" or args.mode is None:
        # Determine which environment config to use
        section = args.section or default_section
        env_cfg = get_neo4j_config_from_env_section(section)

    use_env = False
    if args.mode == "local":
        use_env = False
    elif args.mode == "remote":
        if not env_cfg:
            print("❌ Error: --mode remote specified but no environment variables found")
            return
        use_env = True
    else:
        # Auto-detect: prefer env config if available
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

    # Always verify connection before modifying data.
    success = test_connection(driver, session_kwargs)
    if not success:
        return 1

    if args.test:
        # If user only wants to test the connection, stop here.
        return 0

    with driver.session(**session_kwargs) as session:
        # Remote ingestion always clears first, to ensure a fresh load.
        if use_env:
            session.run("MATCH (n) DETACH DELETE n")
            print("database cleared (remote mode)")
        elif args.clear:
            session.run("MATCH (n) DETACH DELETE n")
            print("database cleared")

        session.execute_write(ingest_fn, kb)
    print("ingestion complete")


if __name__ == "__main__":
    main()
