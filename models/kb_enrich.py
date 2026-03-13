"""Generate assessment datasets using Ollama Qwen Instruct.

This script is focused on the `--generate_qwen` path only.
"""

import argparse
import json
import os
import re
import subprocess

DEFAULT_KB = "../data/knowledge_base/knowledge_base.json"
DEFAULT_OUT = "../data/knowledge_base/generated_qwen.json"
BLOOM_RULE_FILE = "../data/knowledge_base/bloom.json"


def load_json(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def save_json(data: dict, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def call_llm_specific(model: str, prompt: str, timeout: int = 60) -> str:
    """Run a prompt on a specific Ollama model.

    Uses UTF-8 decoding and replaces invalid bytes to avoid
    `UnicodeDecodeError` when reading subprocess output on Windows.
    """
    try:
        proc = subprocess.run(
            ["ollama", "run", model],
            input=prompt,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            timeout=timeout,
        )
        return proc.stdout.strip()
    except subprocess.TimeoutExpired:
        return ""
    except Exception:
        return ""


def generate_assessment_dataset(kb: dict, bloom_data: dict, model: str) -> dict:
    """Generate a complete assessment dataset from scratch using LLM."""
    bloom_keywords = bloom_data.get("bloom_keywords", {})
    distrib = bloom_data.get("distribusi", {})
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
                parts = resp.split("?", 1)
                if len(parts) > 1:
                    soal = parts[0].strip() + "?"
                    rest = parts[1].strip()
                else:
                    soal = resp
                    rest = ""

                opts = []
                lines = rest.split("\n")
                for line in lines:
                    if re.match(r"^[a-d]\)", line.strip(), re.IGNORECASE):
                        opts.append(line.strip())
                if len(opts) < 4:
                    opts = ["a) Option 1", "b) Option 2", "c) Option 3", "d) Option 4"]

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


def fill_missing_options_and_answers(kb: dict, model: str) -> dict:
    """Fill missing `pilihan` (options) and `jawaban` (answer) in a KB.

    Uses the given LLM model to generate plausible multiple-choice options and
    select the correct answer when those fields are missing.
    """

    for unit in kb.get("unit", []):
        concepts = unit.get("konsep", []) or []
        for q in unit.get("evaluasi", []):
            soal = (q.get("soal") or "").strip()
            if not soal:
                continue

            # Fill options if missing/empty
            if not q.get("pilihan"):
                prompt = (
                    f"Create four plausible multiple-choice options (a, b, c, d) for the question:\n"
                    f"{soal}\n"
                    "Format your response like:\n"
                    "a) ...\n"
                    "b) ...\n"
                    "c) ...\n"
                    "d) ...\n"
                )
                if concepts:
                    prompt += f"Use these concepts where relevant: {', '.join(concepts[:5])}.\n"

                out = call_llm_specific(model, prompt)
                opts = []
                for line in out.splitlines():
                    line = line.strip()
                    if re.match(r"^[a-d]\)", line, re.IGNORECASE):
                        opts.append(line)
                if len(opts) < 4:
                    # fallback: try splitting by commas and keep labeled options
                    opts = [o.strip() for o in re.split(r"[\n,]", out) if re.match(r"^[a-d]\)", o.strip(), re.IGNORECASE)]
                if len(opts) < 4:
                    opts = ["a) Option 1", "b) Option 2", "c) Option 3", "d) Option 4"]
                q["pilihan"] = opts

            # Fill answer if missing/empty
            if not q.get("jawaban"):
                if q.get("pilihan"):
                    prompt_ans = (
                        f"Given the question:\n{soal}\n\n"
                        f"And these options:\n{chr(10).join(q['pilihan'])}\n\n"
                        "Which option is correct? Reply with the letter (a/b/c/d) or the full option text."
                    )
                else:
                    prompt_ans = f"Provide the correct short answer for the question:\n{soal}"

                out = call_llm_specific(model, prompt_ans)
                answer = ""

                # Try to extract a letter and map it back to the option list
                m = re.search(r"\b([a-d])\b", out, re.IGNORECASE)
                if m and q.get("pilihan"):
                    letter = m.group(1).lower()
                    for opt in q["pilihan"]:
                        if opt.lower().startswith(f"{letter})"):
                            answer = opt
                            break

                if not answer:
                    # Try extract after 'answer:'
                    if "answer" in out.lower():
                        answer = out.split(":", 1)[-1].strip()
                    else:
                        answer = out.strip()

                if not answer and q.get("pilihan"):
                    answer = q["pilihan"][0]

                q["jawaban"] = answer

    return kb


def generate_with_qwen_instruct(kb_path: str, bloom_path: str, output_path: str):
    kb = load_json(kb_path)
    bloom = load_json(bloom_path)
    generated = generate_assessment_dataset(kb, bloom, "qwen3:4b-instruct")
    save_json(generated, output_path)


def main():
    parser = argparse.ArgumentParser(description="Generate/merge assessment data using Qwen Instruct.")
    parser.add_argument("--input", default=DEFAULT_KB, help="source knowledge base JSON")
    parser.add_argument("--output", default=DEFAULT_OUT, help="destination file for generated data")
    parser.add_argument(
        "--generate_qwen",
        action="store_true",
        help="generate new assessment dataset using Qwen Instruct model",
    )
    parser.add_argument(
        "--fill_missing",
        action="store_true",
        help="fill missing multiple-choice options and answers in the source KB",
    )
    parser.add_argument(
        "--fill_output",
        default="data/knowledge_base/knowledge_base_enriched.json",
        help="output file for the merged/enriched knowledge base JSON",
    )
    args = parser.parse_args()

    if args.generate_qwen:
        generate_with_qwen_instruct(args.input, BLOOM_RULE_FILE, args.output)
        print(f"Generated Qwen dataset to {args.output}")
    elif args.fill_missing:
        kb = load_json(args.input)
        filled = fill_missing_options_and_answers(kb, "qwen3:4b-instruct")
        save_json(filled, args.fill_output)
        print(f"Filled missing fields and wrote merged KB to {args.fill_output}")
    else:
        parser.error("No action specified. Use --generate_qwen or --fill_missing.")


if __name__ == "__main__":
    main()
