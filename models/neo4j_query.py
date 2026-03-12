"""Utility to run an arbitrary Cypher query and persist the results as JSON.

Usage example::

    python models/neo4j_query.py \
        --uri bolt://localhost:7687 \
        --user neo4j \
        --password secret \
        --query "MATCH (u:Unit)-[:HAS_CONCEPT]->(c) RETURN u.kode AS kode, collect(c.text) AS concepts" \
        --output data/knowledge_base/query_results.json

If the query produces multiple rows, each row becomes an element in the
resulting JSON array.  Value objects are converted using the driver's
`record.data()` method, which returns plain Python dicts and lists.
"""

import argparse
import json
from neo4j import GraphDatabase


def run_query(uri, user, password, cypher):
    driver = GraphDatabase.driver(uri, auth=(user, password))
    with driver.session() as session:
        result = session.run(cypher)
        # each record can be turned into a dict via data()
        return [r.data() for r in result]


def main():
    parser = argparse.ArgumentParser(description="Run a Cypher query and dump the results.")
    parser.add_argument("--uri", default="bolt://localhost:7687")
    parser.add_argument("--user", default="neo4j")
    parser.add_argument("--password", default="skkni_mentor")
    parser.add_argument("--query", required=True, help="Cypher query to execute (wrap in quotes)")
    parser.add_argument("--output", default="data/knowledge_base/query_results.json", help="path to write JSON output")
    args = parser.parse_args()

    rows = run_query(args.uri, args.user, args.password, args.query)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    print(f"{len(rows)} row(s) written to {args.output}")


if __name__ == "__main__":
    main()
