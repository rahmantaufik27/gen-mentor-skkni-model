"""Bloom taxonomy tagging utilities.

This module lives under ``src/``; the files it reads are in
``data/processed/`` and the artefacts it generates are placed in
``data/knowledge_base/`` (ontology, knowledge base JSON, etc.).

The workflow used in this project is hybrid:

* a small set of heuristics defined in ``data/processed/bloom.json`` is used
  to identify obvious level cues (keywords such as "apa" or "jelaskan").
* when heuristics cannot decide, a placeholder LLM call is available (stubbed
  here) which can later be replaced with a real model query.
* all automatically generated tags are written back into copies of the
  processed unit JSON files; entries with no tags are marked for review.
* an ontology summary is produced in ``data/knowledge_base/ontology.json``
  (simple node lists rather than a full neo4j dump) so you can inspect before
  ingesting into a graph database.

Tagging is intentionally conservative: multiple levels can be assigned and the
original text remains untouched.
"""

import os
import re
import json
import logging
from typing import Dict, List, Tuple

# same logging style as parsing module
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# paths for various intermediate and output files.  The
# "knowledge base" artifacts (ontology, final JSON, query dumps) live in the
# dedicated folder so they can be versioned or published separately from the
# raw processed units.
BLOOM_RULE_FILE = "data/processed/bloom.json"                   # heuristics input
TAGGED_DIR = "data/processed/tagged"                          # where tagged copies land
ONTOLOGY_FILE = "data/knowledge_base/ontology.json"           # final ontology output


def load_bloom_rules(path: str) -> Tuple[Dict[str, List[str]], Dict[str, int]]:
    """Read the bloom keyword file (JSON) and return (keywords, distribution)."""
    with open(path, encoding="utf-8") as f:
        obj = json.load(f)
    keywords = obj.get("bloom_keywords", {})
    distrib = obj.get("distribusi", {})
    return keywords, distrib


def heuristic_tag(text: str, keywords: Dict[str, List[str]]) -> List[str]:
    """Return a list of levels whose keyword appears in ``text``.

    Matching is case-insensitive and word-boundary-based; if no keyword is
    found, an empty list is returned.
    """
    found = set()
    lower = text.lower()
    for level, kwlist in keywords.items():
        for kw in kwlist:
            if re.search(r"\b" + re.escape(kw.lower()) + r"\b", lower):
                found.add(level)
                break
    return sorted(found)


def llm_tag_stub(text: str) -> List[str]:
    """Placeholder for an LLM-based tagger.

    Currently this stub simply returns the heuristic result again; you can
    replace the body with an actual model call later.
    """
    # TODO: call qwen or other local LLM here
    return []


def tag_unit(data: Dict, keywords: Dict[str, List[str]]) -> Dict:
    """Annotate a single unit JSON dict with bloom tags.

    The returned value is a shallow copy of ``data`` with a new field
    ``_bloom_tags`` added to various substructures to record the levels.
    """
    tagged = dict(data)  # shallow copy of top-level
    # metadata fields
    for field in ("kode_unit", "judul_unit", "deskripsi", "tujuan", "kuk"):
        text = data.get(field, "")
        # some fields may be lists (tujuan); convert to string
        if isinstance(text, list):
            text = " ".join(text)
        if text:
            tags = heuristic_tag(text, keywords)
            tagged.setdefault("_bloom_tags", {})[field] = tags
    # materi
    mat = data.get("materi") or {}
    # ensure the tagged copy has the same structure object so assignments
    # below never KeyError
    tagged.setdefault("materi", {})
    for section in ("topik", "konsep", "proses", "tools"):
        items = mat.get(section, [])
        out = []
        for item in items:
            t = heuristic_tag(item, keywords)
            out.append({"text": item, "bloom": t})
        tagged["materi"][section] = out
    # evaluations: each question already has bloom_level field but we may
    # recompute or augment it
    evals = []
    for q in data.get("evaluasi", []):
        qcopy = dict(q)
        if not qcopy.get("bloom_level"):
            # try heuristics on the question text
            tags = heuristic_tag(qcopy.get("soal", ""), keywords)
            if tags:
                qcopy["bloom_level"] = tags[0]  # choose first match
                qcopy["bloom_tags"] = tags
            else:
                qcopy["bloom_tags"] = []
        else:
            qcopy["bloom_tags"] = [qcopy["bloom_level"]]
        evals.append(qcopy)
    tagged["evaluasi"] = evals
    return tagged


def build_ontology(all_units: List[Dict]) -> Dict:
    """Construct a simple ontology summary from units (processed or tagged).

    The result is flat lists of units/concepts/questions with bloom info when
    available.  This helper is used for writing a companion ``_flat`` file
    regardless of whether the source documents already contained bloom tags.
    """
    ont = {"units": [], "concepts": [], "questions": []}

    for unit in all_units:
        code = unit.get("kode_unit", "")
        ont["units"].append({"code": code, "title": unit.get("judul_unit", ""),
                              "bloom": unit.get("_bloom_tags", {}).get("kode_unit", [])})
        for sec in unit.get("materi", {}).get("konsep", []):
            if isinstance(sec, str):
                text = sec
                bloom = []
            else:
                text = sec.get("text")
                bloom = sec.get("bloom", [])
            ont["concepts"].append({"unit": code, "text": text, "bloom": bloom})
        for q in unit.get("evaluasi", []):
            # questions may be strings or dicts
            if isinstance(q, str):
                text = q
                bloom = []
                qid = None
            else:
                text = q.get("soal")
                bloom = q.get("bloom_tags", []) or ([q.get("bloom_level")] if q.get("bloom_level") else [])
                qid = q.get("id")
            ont["questions"].append({"unit": code, "id": qid, "text": text,
                                      "bloom": bloom})
    return ont


def process_all():
    # load rules
    keywords, distrib = load_bloom_rules(BLOOM_RULE_FILE)
    logger.info("loaded bloom rules, %d levels", len(keywords))

    # read processed units directly; do not alter them
    processed_units = []
    def safe_load(path: str) -> Dict:
        text = open(path, encoding="utf-8").read()
        text = re.sub(r",\s*([\]\}])", r"\1", text)
        return json.loads(text)

    for fname in os.listdir("data/processed"):
        if not fname.lower().endswith(".json"):
            continue
        if fname in (os.path.basename(ONTOLOGY_FILE), os.path.basename(BLOOM_RULE_FILE)):
            continue
        path = os.path.join("data/processed", fname)
        try:
            data = safe_load(path)
        except Exception as exc:
            logger.warning("skipping %s: cannot parse JSON (%s)", fname, exc)
            continue
        processed_units.append(data)

    # build hierarchical ontology from raw processed units
    ontology = build_hierarchical_ontology(processed_units, keywords)
    os.makedirs(os.path.dirname(ONTOLOGY_FILE), exist_ok=True)
    with open(ONTOLOGY_FILE, "w", encoding="utf-8") as f:
        json.dump(ontology, f, ensure_ascii=False, indent=2)
    logger.info("hierarchical ontology written to %s", ONTOLOGY_FILE)

    # also write a flat version for reference
    flat_ont = build_ontology(processed_units)
    flat_path = ONTOLOGY_FILE.replace('.json', '_flat.json')
    with open(flat_path, "w", encoding="utf-8") as f:
        json.dump(flat_ont, f, ensure_ascii=False, indent=2)
    logger.info("flat ontology also saved to %s", flat_path)




def build_hierarchical_ontology(all_units: List[Dict], keywords: Dict[str, List[str]]) -> Dict:
    """Construct a simplified ontology per the latest user spec.

    New structure:
        Unit
          ├─HAS_CONCEPT→ concept_text (no bloom)
          └─HAS_EVALUATION→ evaluation_obj
                   ├─HAS_QUESTION→ soal
                   ├─HAS_OPTION→ pilihan
                   ├─HAS_CORRECT_ANSWER→ jawaban
                   └─HAS_LEVEL→ bloom_level

    Topics are ignored entirely.  Concepts are treated as plain strings and
    any bloom tagging is dropped (they are only supporting material).  Evaluations
    are copied from the unit document; bloom tags from the question text are
    computed if `bloom_level` is missing.
    """
    ont = {"units": []}
    for unit in all_units:
        uobj = {"code": unit.get("kode_unit", ""),
                "title": unit.get("judul_unit", ""),
                "concepts": [],
                "evaluations": []}
        # gather concepts
        for c in unit.get("materi", {}).get("konsep", []):
            # some units store concepts as dicts from prior tagging
            if isinstance(c, dict):
                text = c.get("text", "")
            else:
                text = c
            uobj["concepts"].append(text)
        # gather evaluations
        for q in unit.get("evaluasi", []):
            lvl = q.get("bloom_level")
            if not lvl:
                lvl = heuristic_tag(q.get("soal", ""), keywords)
                lvl = lvl[0] if lvl else ""
            uobj["evaluations"].append({
                "id": q.get("id"),
                "soal": q.get("soal"),
                "pilihan": q.get("pilihan", []),
                "jawaban": q.get("jawaban", ""),
                "bloom_level": lvl,
            })
        ont["units"].append(uobj)
    return ont


if __name__ == "__main__":
    process_all()
