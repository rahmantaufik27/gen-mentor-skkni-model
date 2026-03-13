"""Helpers for sampling evaluation questions from a Neo4j graph.

This module encapsulates the Bloom/level-distributed query discussed in the
conversation.  It can be imported by other code or executed as a script.

Functions
---------
* ``build_bloom_query(distribution)`` -- construct the UNION ALL query for a
  given level->count mapping.
* ``run_query(uri, user, password, cypher)`` -- same as neo4j_query but
  returned rows are handed back directly.
* ``sample_bloom(uri, user, password, distribution)`` -- build and execute the
  bloom query, returning the row list.

Command-line usage mirrors ``neo4j_query.py`` but accepts a JSON file or
literal string for the distribution map.  Example::

    python models/neo4j_sampler.py \
        --distribution '{"C1":1,"C2":2,"C3":3,"C4":2,"C5":1,"C6":1}' \
        --output data/knowledge_base/bloom_sample.json

If you prefer, call ``sample_bloom()`` from Python code:

    from src.neo4j_sampler import sample_bloom
    dist = {"C1":1, "C2":2, "C3":3, "C4":2, "C5":1, "C6":1}
    rows = sample_bloom('bolt://localhost:7687', 'neo4j', 'secret', dist)

"""

import argparse
import json
from neo4j import GraphDatabase


def build_bloom_query(distribution: dict) -> str:
    """Return a Cypher string sampling questions by bloom level.

    ``distribution`` maps bloom levels (strings) to integer counts.  The
    generated query uses ``UNION ALL`` to concatenate one subquery per level
    and includes ``ORDER BY rand()`` and a per-unit deduplication step so that
    each returned question comes from a different unit when possible.
    """

    parts = []
    for level, cnt in distribution.items():
        try:
            cnt = int(cnt)
        except Exception:
            continue
        if cnt <= 0:
            continue
        part = (
            f"MATCH (u:Unit)-[:HAS_EVALUATION]->(e:Evaluation)\n"
            f"WHERE e.bloom_level = '{level}'\n"
            # choose one random evaluation per unit
            "WITH u, e, rand() AS r\n"
            "ORDER BY u.kode, r\n"
            "WITH u, head(collect(e)) AS e\n"
            # gather concepts and options after we've picked the evaluation
            "OPTIONAL MATCH (u)-[:HAS_CONCEPT]->(c:Concept)\n"
            "OPTIONAL MATCH (e)-[:HAS_OPTION]->(o:Option)\n"
            "RETURN u.kode AS unit, collect(distinct c.text) AS concepts,\n"
            "       e.soal AS soal, e.jawaban AS jawaban,\n"
            "       collect(distinct o.text) AS options,\n"
            "       e.bloom_level AS bloom\n"
            f"LIMIT {cnt}"
        )
        parts.append(part)
    if not parts:
        return ""  # nothing to run
    return "\nUNION ALL\n".join(parts) + ";"


def run_query(uri: str, user: str, password: str, cypher: str):
    driver = GraphDatabase.driver(uri, auth=(user, password))
    with driver.session() as session:
        result = session.run(cypher)
        return [r.data() for r in result]


def sample_bloom(uri: str, user: str, password: str, distribution: dict):
    """Build and execute a Bloom-distributed sampling query.

    Returns the list of result rows (dictionaries).
    """

    cypher = build_bloom_query(distribution)
    if not cypher:
        return []
    return run_query(uri, user, password, cypher)


def main():
    parser = argparse.ArgumentParser(description="Sample questions by Bloom level.")
    parser.add_argument("--uri", default="bolt://localhost:7687")
    parser.add_argument("--user", default="neo4j")
    parser.add_argument("--password", default="skkni_mentor")
    parser.add_argument(
        "--distribution",
        required=True,
        help="JSON map of bloom levels to counts (e.g. '{\"C1\":1,\"C2\":2}')",
    )
    parser.add_argument(
        "--output",
        default="data/knowledge_base/bloom_sample.json",
        help="file to write results as JSON (defaults into data/knowledge_base)",
    )
    args = parser.parse_args()

    try:
        dist = json.loads(args.distribution)
    except Exception:
        raise SystemExit("invalid JSON for --distribution")

    rows = sample_bloom(args.uri, args.user, args.password, dist)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    print(f"{len(rows)} row(s) written to {args.output}")


if __name__ == "__main__":
    main()
