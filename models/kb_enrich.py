"""Enrich a knowledge base JSON by filling missing fields or generate new assessment datasets.

This script has multiple modes:

1. **Enrichment Mode** (default): Reads the existing file (default `data/knowledge_base/knowledge_base.json`),
   examines each evaluation entry and, when one of the optional attributes is absent or empty,
   tries to synthesise a value using heuristics and LLM calls.

2. **Generation Mode (Zero-Context)**: Generates complete new assessment datasets from scratch using specified LLMs,
   based solely on unit titles and Bloom taxonomy levels, without referencing existing concepts.

3. **Generation Mode (With Concepts)**: Generates datasets using concepts, units, existing question patterns (few-shot),
   and Bloom keywords for guidance.

Usage (from project root):

    # Enrichment
    python models/kb_enrich.py \
        --input data/knowledge_base/knowledge_base.json \
        --output data/knowledge_base/knowledge_base_enriched.json \
        --add_levels

    # Generate zero-context with Qwen Instruct
    python models/kb_enrich.py --generate_qwen --qwen_output data/knowledge_base/generated_qwen.json

    # Generate zero-context with Thinking Llama
    python models/kb_enrich.py --generate_llama --llama_output data/knowledge_base/generated_llama.json

    # Generate with concepts using Qwen 4B
    python models/kb_enrich.py --generate_qwen4b --qwen4b_output data/knowledge_base/generated_qwen4b.json

    # Generate with concepts using Qwen 8B
    python models/kb_enrich.py --generate_qwen8b --qwen8b_output data/knowledge_base/generated_qwen8b.json

The script uses Ollama for LLM calls and can be extended with more models or prompts.
"""

import argparse
import json
import os
import re
import subprocess
import sys
from typing import Any, Dict, List

# allow importing the tagging helpers from src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.tagging import heuristic_tag, load_bloom_rules

DEFAULT_KB = "data/knowledge_base/knowledge_base.json"
DEFAULT_OUT = "data/knowledge_base/knowledge_base_enriched.json"
BLOOM_RULE_FILE = "data/knowledge_base/bloom.json"  # moved during refactor


def load_kb(path: str) -> Dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def save_kb(kb: Dict[str, Any], path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(kb, f, ensure_ascii=False, indent=2)


def load_bloom(path: str) -> Dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def call_llm_specific(model: str, prompt: str) -> str:
    """Run a prompt on a specific Ollama model.

    Failures return an empty string.
    """
    try:
        proc = subprocess.run(
            ["ollama", "run", model],
            input=prompt,
            text=True,
            capture_output=True,
            timeout=30,
        )
        return proc.stdout.strip()
    except Exception:
        return ""


def generate_assessment_dataset(kb: Dict[str, Any], bloom_data: Dict[str, Any], model: str) -> Dict[str, Any]:
    """Generate a complete assessment dataset from scratch using LLM.

    For each unit's title and each Bloom level, create the specified number
    of questions based on the model's internal knowledge. Does not use
    existing concepts or evaluations.
    """
    bloom_keywords = bloom_data["bloom_keywords"]
    distrib = bloom_data["distribusi"]
    new_kb = {"unit": []}
    for unit in kb.get("unit", []):
        title = unit.get("judul_unit", "")
        new_unit = {"kode_unit": unit.get("kode_unit"), "judul_unit": title, "evaluasi": []}
        for level, num in distrib.items():
            keywords = bloom_keywords.get(level, [])
            for _ in range(num):
                prompt = (
                    f"Using your knowledge, create a Bloom taxonomy level {level} "
                    f"multiple-choice question about the topic '{title}'. "
                    f"The question should involve actions like {', '.join(keywords[:3])}. "
                    "Provide the question, four options labeled a), b), c), d), "
                    "and the correct answer."
                )
                resp = call_llm_specific(model, prompt)
                # Parse response
                parts = resp.split("?", 1)
                if len(parts) > 1:
                    soal = parts[0].strip() + "?"
                    rest = parts[1].strip()
                else:
                    soal = resp
                    rest = ""
                # Extract options
                opts = []
                lines = rest.split("\n")
                for line in lines:
                    if re.match(r"^[a-d]\)", line.strip(), re.IGNORECASE):
                        opts.append(line.strip())
                if len(opts) < 4:
                    opts = ["a) Option 1", "b) Option 2", "c) Option 3", "d) Option 4"]
                # Extract answer
                answer = ""
                for line in lines:
                    if "correct" in line.lower() or "answer:" in line.lower():
                        answer = line.split(":", 1)[-1].strip()
                        break
                if not answer and opts:
                    answer = opts[0]
                new_q = {
                    "bloom_level": level,
                    "soal": soal,
                    "pilihan": opts,
                    "jawaban": answer,
                }
                new_unit["evaluasi"].append(new_q)
        new_kb["unit"].append(new_unit)
    return new_kb


def generate_with_qwen_instruct(kb_path: str, bloom_path: str, output_path: str):
    kb = load_kb(kb_path)
    bloom = load_bloom(bloom_path)
    generated = generate_assessment_dataset(kb, bloom, "qwen3:4b-instruct")
    save_kb(generated, output_path)


def generate_with_thinking_llama(kb_path: str, bloom_path: str, output_path: str):
    kb = load_kb(kb_path)
    bloom = load_bloom(bloom_path)
    generated = generate_assessment_dataset(kb, bloom, "llama3.1:8b-instruct")
    save_kb(generated, output_path)


def get_few_shot_examples(unit: Dict[str, Any], level: str, num_examples: int = 1) -> str:
    """Extract few-shot examples from existing evaluations for the given Bloom level."""
    examples = [q for q in unit.get("evaluasi", []) if q.get("bloom_level") == level]
    if not examples:
        return ""
    selected = examples[:num_examples]
    ex_str = "Examples of similar questions:\n"
    for ex in selected:
        ex_str += f"Question: {ex.get('soal', '')}\n"
        ex_str += f"Options: {' '.join(ex.get('pilihan', []))}\n"
        ex_str += f"Answer: {ex.get('jawaban', '')}\n\n"
    return ex_str


def generate_with_concepts_dataset(kb: Dict[str, Any], bloom_data: Dict[str, Any], model: str) -> Dict[str, Any]:
    """Generate assessment dataset using concepts, units, and few-shot examples."""
    bloom_keywords = bloom_data["bloom_keywords"]
    distrib = bloom_data["distribusi"]
    new_kb = {"unit": []}
    for unit in kb.get("unit", []):
        title = unit.get("judul_unit", "")
        concepts = unit.get("konsep", [])
        new_unit = {"kode_unit": unit.get("kode_unit"), "judul_unit": title, "evaluasi": []}
        for level, num in distrib.items():
            keywords = bloom_keywords.get(level, [])
            few_shot = get_few_shot_examples(unit, level, 1)
            for _ in range(num):
                prompt = (
                    f"Topic: {title}\n"
                    f"Concepts: {', '.join(concepts[:5])}\n"
                    f"Bloom Level: {level} (involves actions like {', '.join(keywords[:3])})\n"
                    f"{few_shot}"
                    "Generate a new multiple-choice question at this Bloom level, using the concepts and following the example style.\n"
                    "Format your response as:\n"
                    "Question: [the question text]\n"
                    "Options: a) [option1] b) [option2] c) [option3] d) [option4]\n"
                    "Answer: [the correct option, e.g., a)]\n"
                )
                resp = call_llm_specific(model, prompt)
                # Parse response
                soal = ""
                opts = []
                answer = ""
                lines = resp.split("\n")
                for line in lines:
                    line = line.strip()
                    if line.startswith("Question:"):
                        soal = line.replace("Question:", "").strip()
                    elif line.startswith("Options:"):
                        opts_str = line.replace("Options:", "").strip()
                        opts = re.split(r'\s*[a-d]\)\s*', opts_str)[1:]
                        opts = [f"{chr(97+i)}) {opt.strip()}" for i, opt in enumerate(opts[:4])]
                    elif line.startswith("Answer:"):
                        answer = line.replace("Answer:", "").strip()
                if not soal:
                    soal = "Generated question"
                if len(opts) < 4:
                    opts = ["a) Option 1", "b) Option 2", "c) Option 3", "d) Option 4"]
                if not answer:
                    answer = opts[0] if opts else "a) Option 1"
                new_q = {
                    "bloom_level": level,
                    "soal": soal,
                    "pilihan": opts,
                    "jawaban": answer,
                }
                new_unit["evaluasi"].append(new_q)
        new_kb["unit"].append(new_unit)
    return new_kb


def generate_with_qwen4b(kb_path: str, bloom_path: str, output_path: str):
    kb = load_kb(kb_path)
    bloom = load_bloom(bloom_path)
    generated = generate_with_concepts_dataset(kb, bloom, "qwen3:4b-instruct")
    save_kb(generated, output_path)


def generate_with_qwen8b(kb_path: str, bloom_path: str, output_path: str):
    kb = load_kb(kb_path)
    bloom = load_bloom(bloom_path)
    generated = generate_with_concepts_dataset(kb, bloom, "qwen3:8b-instruct")
    save_kb(generated, output_path)


def select_best_model() -> str:
    """Pick the 'best' Qwen-style model available via `ollama list`.

    We look for any model whose name starts with ``qwen`` and choose the one
    with the highest lexical sort order as a crude proxy for version. If the
    list command fails we fall back to a hardcoded default.
    """
    try:
        proc = subprocess.run(
            ["ollama", "list"],
            text=True,
            capture_output=True,
            timeout=10,
        )
        lines = proc.stdout.splitlines()
        qwen_models = [l.split()[0] for l in lines if l.startswith("qwen")]
        if qwen_models:
            # choose the "largest" string, e.g. qwen3:4b-instruct > qwen2:2b
            return sorted(qwen_models)[-1]
    except Exception:
        pass
    return "qwen3:4b-instruct"  # sensible default


LLM_MODEL = select_best_model()


def call_llm(prompt: str) -> str:
    """Run a prompt on the local Ollama model selected above.

    Failures return an empty string so callers can fall back to other
    heuristics without crashing.
    """
    try:
        proc = subprocess.run(
            ["ollama", "run", LLM_MODEL],
            input=prompt,
            text=True,
            capture_output=True,
            timeout=20,
        )
        return proc.stdout.strip()
    except Exception:
        return ""


def _fuzzy_answer(unit_title: str, soal: str, concepts: List[str]) -> str:
    """Attempt to guess an answer using fuzzy matching before calling the LLM.

    If any concept word (or portion of the unit title) shows up in the
    question text, return that as a candidate. Otherwise return empty string.
    """
    lower = soal.lower()
    for concept in concepts:
        if concept.lower() in lower:
            return concept
    # split title into words and look for overlap
    for word in unit_title.split():
        if word.lower() in lower:
            return word
    return ""


def enrich_evaluations(kb: Dict[str, Any], add_levels: bool = False) -> Dict[str, Any]:
    """Walk units and evaluations, filling blanks where possible.

    * When an answer or option is missing try a cheap fuzzy/regex heuristic
      based on the unit title and any concept list. This lets us capture
      repeated patterns where the question literally echoes the title or
      concept names without invoking the LLM.
    * If heuristics fail we fallback to the Ollama model selected by
      :func:`select_best_model`.
    * When ``add_levels`` is True we also synthesise questions for any Bloom
      levels not already present in the unit, again using the LLM.
    """

    keywords, _ = load_bloom_rules(BLOOM_RULE_FILE)
    all_levels = [f"C{i}" for i in range(1, 7)]

    for unit in kb.get("unit", []):
        title = unit.get("judul", "")
        concepts: List[str] = unit.get("konsep", []) or []
        present_levels = set()
        for q in unit.get("evaluasi", []):
            soal = q.get("soal", "")
            lvl = q.get("bloom_level")
            if lvl:
                present_levels.add(lvl)
            # answer
            if not q.get("jawaban"):
                guessed = _fuzzy_answer(title, soal, concepts)
                if guessed:
                    q["jawaban"] = guessed
                else:
                    if concepts:
                        prompt = (
                            f"Generate a correct short answer for the following question '{soal}' "
                            f"using these concepts: {', '.join(concepts[:4])}."
                        )
                        out = call_llm(prompt)
                        q["jawaban"] = out or concepts[0]
                    else:
                        q["jawaban"] = call_llm(f"Answer: {soal}")
            # options
            if not q.get("pilihan"):
                if concepts:
                    prompt = (
                        f"Provide four plausible multiple-choice options for '{soal}' "
                        f"based on these concepts: {', '.join(concepts[:4])}."
                    )
                    out = call_llm(prompt)
                    if out:
                        opts = [s.strip() for s in re.split(r"[\n,]", out) if s.strip()]
                        q["pilihan"] = opts[:4]
                    else:
                        q["pilihan"] = concepts[:4]
                else:
                    q["pilihan"] = []
            # bloom level / tags
            if not lvl:
                tags = heuristic_tag(soal, keywords)
                if tags:
                    q["bloom_level"] = tags[0]
                    present_levels.add(q["bloom_level"])
        # optionally generate missing level questions
        if add_levels:
            missing = [l for l in all_levels if l not in present_levels]
            for lvl in missing:
                if not concepts:
                    continue
                prompt_q = (
                    f"Create a Bloom-level {lvl} multiple-choice question about "
                    f"these concepts: {', '.join(concepts[:4])}. Provide the question, "
                    "four options separated by commas, and indicate the correct answer."
                )
                resp = call_llm(prompt_q)
                if resp:
                    parts = [p.strip() for p in resp.split("?", 1)]
                    if len(parts) > 1:
                        question_text = parts[0] + "?"
                        rest = parts[1]
                    else:
                        question_text = resp
                        rest = ""
                    opts = []
                    answer = ""
                    if "Answer:" in rest or "Jawaban" in rest:
                        segs = re.split(r"Answer:|Jawaban:|Correct answer:", rest)
                        if len(segs) > 1:
                            answer = segs[1].strip().split("\n")[0]
                        rest = segs[0]
                    opts = [o.strip() for o in rest.split(",") if o.strip()]
                    if not opts and concepts:
                        opts = concepts[:4]
                    if not answer and opts:
                        answer = opts[0]
                else:
                    question_text = f"(generated question about {concepts[0]})"
                    opts = concepts[:4]
                    answer = concepts[0]
                new_q = {
                    "bloom_level": lvl,
                    "soal": question_text,
                    "pilihan": opts,
                    "jawaban": answer,
                }
                unit.setdefault("evaluasi", []).append(new_q)
    return kb


def main():
    parser = argparse.ArgumentParser(description="Fill missing fields in a KB JSON file or generate new assessment datasets.")
    parser.add_argument("--input", default=DEFAULT_KB, help="source knowledge base JSON")
    parser.add_argument("--output", default=DEFAULT_OUT, help="destination file for enriched KB")
    parser.add_argument(
        "--add_levels",
        action="store_true",
        help="also synthesise questions for any missing Bloom levels",
    )
    parser.add_argument(
        "--generate_qwen",
        action="store_true",
        help="generate new assessment dataset using Qwen Instruct model",
    )
    parser.add_argument(
        "--generate_llama",
        action="store_true",
        help="generate new assessment dataset using Thinking Llama model",
    )
    parser.add_argument(
        "--generate_qwen4b",
        action="store_true",
        help="generate new assessment dataset using Qwen 4B model with concepts",
    )
    parser.add_argument(
        "--generate_qwen8b",
        action="store_true",
        help="generate new assessment dataset using Qwen 8B model with concepts",
    )
    parser.add_argument(
        "--qwen_output",
        default="data/knowledge_base/generated_qwen.json",
        help="output file for Qwen-generated dataset",
    )
    parser.add_argument(
        "--llama_output",
        default="data/knowledge_base/generated_llama.json",
        help="output file for Llama-generated dataset",
    )
    parser.add_argument(
        "--qwen4b_output",
        default="data/knowledge_base/generated_qwen4b.json",
        help="output file for Qwen 4B-generated dataset",
    )
    parser.add_argument(
        "--qwen8b_output",
        default="data/knowledge_base/generated_qwen8b.json",
        help="output file for Qwen 8B-generated dataset",
    )
    args = parser.parse_args()

    if args.generate_qwen:
        generate_with_qwen_instruct(args.input, BLOOM_RULE_FILE, args.qwen_output)
        print(f"Generated Qwen dataset to {args.qwen_output}")
    elif args.generate_llama:
        generate_with_thinking_llama(args.input, BLOOM_RULE_FILE, args.llama_output)
        print(f"Generated Llama dataset to {args.llama_output}")
    elif args.generate_qwen4b:
        generate_with_qwen4b(args.input, BLOOM_RULE_FILE, args.qwen4b_output)
        print(f"Generated Qwen 4B dataset to {args.qwen4b_output}")
    elif args.generate_qwen8b:
        generate_with_qwen8b(args.input, BLOOM_RULE_FILE, args.qwen8b_output)
        print(f"Generated Qwen 8B dataset to {args.qwen8b_output}")
    else:
        # Default enrichment behavior
        kb = load_kb(args.input)
        enriched = enrich_evaluations(kb, add_levels=args.add_levels)
        save_kb(enriched, args.output)
        print(
            f"wrote enriched knowledge base with {len(enriched.get('unit', []))} units to {args.output}"
        )


if __name__ == "__main__":
    main()
