"""Parser for SKKNI PDF slides.

This script walks through a folder of PDF files, extracts text via pdfplumber,
segregates the content into semantic sections using keyword heuristics, and
serializes each unit into a JSON document suitable for later tagging,
question-generation, evaluation and remediation.

Design goals:
* modular functions for each step (listing, extraction, section parsing, normal-
  ization)
* flexible section detection with regular-expression keywords
* simple logging to track progress
* batch processing of multiple files
* incremental behaviour: existing JSON files are not overwritten unless the
  corresponding PDF is newer
* schema aligned with the latest requirements, including metadata,
  materi/evaluasi/referensi fields

Usage::

    python -m src.parsing --input data/raw/skkni --output data/processed

"""

import os
import re
import json
import logging
from pathlib import Path
from typing import List, Dict

import pdfplumber

# logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# regex patterns -> normalized section names
SECTION_KEYWORDS = {
    r"Kode Unit": "kode_unit",
    r"Unit Kompetensi": "judul_unit",
    r"Deskripsi": "deskripsi",
    r"Tujuan": "tujuan",
    r"Kriteria Unjuk Kerja": "kuk",
    r"Materi": "materi",
    r"Topik": "materi",
    r"Teknologi": "tools",
    r"Tools": "tools",
    r"Langkah": "proses",
    r"Soal": "soal",
    r"Latihan": "latihan",
    r"Studi Kasus": "studi_kasus",
    r"Quiz": "quiz",
    r"Pre[- ]?test": "pre_test",
    r"Post[- ]?test": "post_test",
    r"Referensi": "referensi",
}


def list_pdf_files(directory: str) -> List[str]:
    """Return sorted list of PDF file paths under ``directory``."""
    pdfs = []
    for entry in os.listdir(directory):
        if entry.lower().endswith(".pdf"):
            pdfs.append(os.path.join(directory, entry))
    pdfs.sort()
    logger.info("found %d PDF files in %s", len(pdfs), directory)
    return pdfs


def extract_text_from_pdf(path: str) -> str:
    """Return the concatenated text of all pages in ``path``.

    Any exceptions are caught and an empty string is returned, allowing the
    batch job to continue.
    """
    try:
        with pdfplumber.open(path) as pdf:
            text = "\n".join(p.extract_text() or "" for p in pdf.pages)
        logger.debug("extracted %d characters from %s", len(text), path)
        return text
    except Exception as exc:
        logger.error("error reading %s: %s", path, exc)
        return ""


def parse_sections(text: str) -> Dict[str, List[str]]:
    """Segment ``text`` into lists of lines per section keyword.

    A simple state machine: when a line matches a keyword pattern the current
    section is switched. Following lines are appended to that section until a
    new keyword appears. Case-insensitive.
    """
    result = {v: [] for v in SECTION_KEYWORDS.values()}
    current = None

    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue

        # check for a keyword in this line
        matched = False
        for pattern, name in SECTION_KEYWORDS.items():
            if re.search(pattern, line, flags=re.IGNORECASE):
                current = name
                matched = True
                # capture trailing text after keyword
                after = re.split(pattern, line, flags=re.IGNORECASE)[-1].strip()
                if after:
                    result[current].append(after)
                break
        if not matched and current:
            result[current].append(line)
    return result


def parse_questions(lines: List[str]) -> List[Dict]:
    """Convert a flat list of evaluation lines into question objects.

    A very simple heuristic parser: any line beginning with "Pertanyaan" or
    "Soal" starts a new question. Subsequent lines that look like options
    (e.g. "a) ..." or "b.") are collected under ``pilihan``. Lines containing
    the word "jawaban" are interpreted as the answer. Remaining text is
    appended to the question prompt itself. Bloom level is left blank for now;
    it will be filled later by the tagging module.
    """
    questions: List[Dict] = []
    current = None

    for line in lines:
        if re.match(r"^(Pertanyaan|Soal)\b", line, flags=re.IGNORECASE):
            if current:
                questions.append(current)
            current = {
                "id": len(questions) + 1,
                "bloom_level": "",
                "soal": line.strip(),
                "pilihan": [],
                "jawaban": "",
            }
        elif current:
            # option detection
            if re.match(r"^[a-d]\)|^[a-d]\.", line, flags=re.IGNORECASE):
                current["pilihan"].append(line.strip())
            elif "jawaban" in line.lower():
                parts = line.split(":", 1)
                if len(parts) > 1:
                    current["jawaban"] = parts[1].strip()
            else:
                # append to question text if not an option or answer
                current["soal"] += " " + line.strip()
    if current:
        questions.append(current)
    return questions


def normalize_parsed(parsed: Dict[str, List[str]]) -> Dict:
    """Map raw sections to the final JSON schema."""
    obj = {
        "kode_unit": "",
        "judul_pelatihan": "",
        "deskripsi": "",
        "tujuan": "",
        "kuk": "",
        "materi": {
            "topik": [],
            "konsep": [],
            "proses": [],
            "tools": [],
        },
        # evaluations will be a list of question dicts
        "evaluasi": [],
        "referensi": [],
    }

    # simple joins for single‑value metadata
    for field in ("kode_unit", "judul_unit", "deskripsi", "tujuan", "kuk"):
        if parsed.get(field):
            obj[field] = " ".join(parsed[field]).strip()

    # materi sections are treated as lists; additional heuristics could be added
    if parsed.get("materi"):
        # split on numbered bullets or semicolons; the resulting items often
        # include both topic headings and conceptual details.
        content = " ".join(parsed["materi"])
        items = re.split(r"\d+\.|;", content)
        items = [i.strip() for i in items if i.strip()]
        # for now we treat the same list as both `topik` and `konsep`; this
        # ensures the hierarchical ontology has nonempty topic entries.  Later
        # we can refine by separating headings from explanatory text.
        obj["materi"]["topik"] = items
        obj["materi"]["konsep"] = items
    if parsed.get("tools"):
        obj["materi"]["tools"] = parsed["tools"]
    if parsed.get("proses"):
        obj["materi"]["proses"] = parsed["proses"]

    # collect all evaluation-related lines together to be interpreted as
    # question objects; this flattens pre-test, quiz, post-test, tugas, dll.
    eval_lines: List[str] = []
    for key in ("tugas", "latihan", "studi_kasus", "pre_test", "quiz", "post_test", "soal"):
        eval_lines.extend(parsed.get(key, []))

    # build evaluation list; parse_questions already handles the lines and
    # returns an empty list if nothing is found, so assignment is safe.
    obj["evaluasi"] = parse_questions(eval_lines)

    obj["referensi"] = parsed.get("referensi", [])
    return obj


def parse_pdf_file(pdf_path: str) -> Dict:
    logger.info("parsing %s", pdf_path)
    txt = extract_text_from_pdf(pdf_path)
    if not txt:
        return {}
    sections = parse_sections(txt)
    return normalize_parsed(sections)


def process_all(input_dir: str, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)
    pdfs = list_pdf_files(input_dir)
    for pdf in pdfs:
        stem = Path(pdf).stem.replace(" ", "_")
        out_path = os.path.join(output_dir, f"{stem}.json")
        # incremental: skip if output exists and is newer than pdf
        if os.path.exists(out_path):
            pdf_mtime = os.path.getmtime(pdf)
            json_mtime = os.path.getmtime(out_path)
            if json_mtime >= pdf_mtime:
                logger.info("skipping %s (already parsed)", pdf)
                continue
        data = parse_pdf_file(pdf)
        if data:
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info("wrote %s", out_path)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="parse SKKNI PDF slides into structured.json")
    parser.add_argument("--input", default="data/raw/skkni", help="folder of pdf files")
    parser.add_argument("--output", default="data/processed", help="output folder for json")
    args = parser.parse_args()
    process_all(args.input, args.output)
