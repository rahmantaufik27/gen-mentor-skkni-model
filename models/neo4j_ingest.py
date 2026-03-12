"""Simple Neo4j ingestion helper for knowledge_base.json.

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

    python models/neo4j_ingest.py \
        --uri bolt://localhost:7687 \
        --user neo4j \
        --password secret \
        --file data/knowledge_base/knowledge_base.json

If the database already contains nodes you may want to clear it first. The
script accepts a ``--clear`` flag which will run ``MATCH (n) DETACH DELETE n``
before ingesting.
"""

import json
import argparse
from neo4j import GraphDatabase


def load_kb(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def ingest(tx, kb):
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
    parser = argparse.ArgumentParser(description="Ingest knowledge base JSON into Neo4j.")
    parser.add_argument("--uri", default="bolt://localhost:7687")
    parser.add_argument("--user", default="neo4j")
    parser.add_argument("--password", default="skkni_mentor")
    # input knowledge base JSON is now expected under data/knowledge_base
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
    print("ingestion complete")


if __name__ == "__main__":
    main()
