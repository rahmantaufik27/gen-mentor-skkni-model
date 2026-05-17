import argparse
import difflib
import json
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime


def log_info(message: str):
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] INFO: {message}", flush=True)


def log_error(message: str):
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] ERROR: {message}", flush=True)


def normalize_text(value):
    if value is None:
        return ""
    if isinstance(value, str):
        return " ".join(value.strip().split())
    return str(value).strip()


def canonicalize_unit_code(unit):
    if not unit:
        return ""
    normalized = normalize_text(unit).upper()
    normalized = normalized.replace("_", ".")
    normalized = re.sub(r"[^A-Z0-9\.]+", "", normalized)
    normalized = re.sub(r"\.{2,}", ".", normalized)
    normalized = normalized.strip(".")
    return normalized


def load_canonical_units():
    kb_path = os.path.join("data", "knowledge_base", "knowledge_base_fix.json")
    if not os.path.isfile(kb_path):
        return set()

    try:
        with open(kb_path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception:
        return set()

    unit_codes = set()
    for unit in payload.get("unit", []):
        code = unit.get("kode_unit")
        if code:
            unit_codes.add(canonicalize_unit_code(code))
    return unit_codes


def map_unit_to_canonical(unit, canonical_units):
    unit_key = canonicalize_unit_code(unit)
    if not unit_key:
        return "<MISSING UNIT>"
    if unit_key in canonical_units:
        return unit_key

    stripped = re.sub(r"[^A-Z0-9]", "", unit_key)
    if stripped:
        best_match = None
        best_ratio = 0.0
        for canonical in canonical_units:
            canonical_stripped = re.sub(r"[^A-Z0-9]", "", canonical)
            if canonical_stripped == stripped:
                return canonical
            if stripped.startswith(canonical_stripped) or canonical_stripped.startswith(stripped):
                return canonical
            seq_ratio = difflib.SequenceMatcher(None, stripped, canonical_stripped).ratio()
            if seq_ratio > best_ratio:
                best_ratio = seq_ratio
                best_match = canonical
        if best_match and best_ratio >= 0.75:
            return best_match

    # fallback to fuzzy matching on normalized unit string
    close_names = difflib.get_close_matches(unit_key, canonical_units, n=1, cutoff=0.70)
    if close_names:
        return close_names[0]

    return unit_key


def resolve_input_file(input_param):
    if os.path.isfile(input_param):
        return input_param

    candidate = os.path.join("data", "generated_question", input_param)
    if os.path.isfile(candidate):
        return candidate

    candidate = os.path.join("data", "evaluasi", input_param)
    if os.path.isfile(candidate):
        return candidate

    raise FileNotFoundError(f"Input file not found: {input_param}")


def load_question_entries(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        if "questions" in payload and isinstance(payload["questions"], list):
            return payload["questions"]
        if "data" in payload and isinstance(payload["data"], list):
            return payload["data"]
        if "items" in payload and isinstance(payload["items"], list):
            return payload["items"]

    raise ValueError("Unsupported JSON structure: expected a list or a top-level object with a list value.")


def summarize_generated_questions(entries, canonical_units=None):
    canonical_units = canonical_units or set()
    total = len(entries)
    unit_counts = Counter()
    bloom_counts = Counter()
    bloom_by_unit = defaultdict(Counter)
    exact_duplicates = Counter()
    stem_duplicates = Counter()
    invalid_options_counts = 0
    missing_fields = Counter()
    difficulty_counts = Counter()
    status_counts = Counter()
    options_size_counts = Counter()
    unknown_fields = Counter()

    for item in entries:
        if not isinstance(item, dict):
            missing_fields["invalid_item_type"] += 1
            continue

        question = normalize_text(item.get("question") or item.get("stem") or item.get("prompt"))
        raw_options = item.get("options") or item.get("choices") or item.get("jawaban_choices")
        if not isinstance(raw_options, list):
            raw_options = []

        options = [normalize_text(opt) for opt in raw_options if normalize_text(opt)]
        correct_answer = normalize_text(item.get("correct_answer") or item.get("answer") or item.get("jawaban"))
        bloom_level = normalize_text(item.get("bloom_level") or item.get("bloom") or item.get("bloom_level_raw"))
        unit = normalize_text(item.get("unit") or item.get("unit_code") or item.get("kode_unit") or item.get("topic"))
        difficulty = normalize_text(item.get("difficulty") or item.get("level"))
        status = normalize_text(item.get("status") or item.get("review_status") or item.get("state"))

        canonical_unit = map_unit_to_canonical(unit, canonical_units)

        if not question:
            missing_fields["question"] += 1
        if not options:
            missing_fields["options"] += 1
        if not correct_answer:
            missing_fields["correct_answer"] += 1
        if not bloom_level:
            missing_fields["bloom_level"] += 1
        if not unit:
            missing_fields["unit"] += 1

        if not options:
            options_size_counts[0] += 1
        else:
            options_size_counts[len(options)] += 1

        if question:
            stem_duplicates[question] += 1

        key = (question, tuple(options))
        exact_duplicates[key] += 1

        normalized_unit = canonical_unit or "<MISSING UNIT>"
        unit_counts[normalized_unit] += 1
        if bloom_level:
            bloom_counts[bloom_level] += 1
            bloom_by_unit[normalized_unit][bloom_level] += 1

        if difficulty:
            difficulty_counts[difficulty] += 1
        if status:
            status_counts[status] += 1

        if correct_answer and options:
            normalized_option_labels = [opt[0].upper() if opt else "" for opt in options]
            if correct_answer.upper() not in normalized_option_labels:
                invalid_options_counts += 1

        for key_name in ["question", "options", "correct_answer", "bloom_level", "unit", "difficulty", "status"]:
            if key_name not in item:
                unknown_fields[f"missing_{key_name}"] += 0

    duplicate_exact_groups = {k: v for k, v in exact_duplicates.items() if v > 1}
    duplicate_stem_groups = {k: v for k, v in stem_duplicates.items() if v > 1}

    return {
        "total": total,
        "unit_counts": unit_counts,
        "bloom_counts": bloom_counts,
        "bloom_by_unit": bloom_by_unit,
        "exact_duplicate_groups": duplicate_exact_groups,
        "stem_duplicate_groups": duplicate_stem_groups,
        "invalid_options_counts": invalid_options_counts,
        "missing_fields": missing_fields,
        "difficulty_counts": difficulty_counts,
        "status_counts": status_counts,
        "options_size_counts": options_size_counts,
    }


def format_counter(counter, limit=None):
    if not counter:
        return "-"
    items = counter.most_common(limit) if limit else counter.items()
    return "; ".join([f"{key}: {count}" for key, count in items])


def log_summary(summary, file_path):
    log_info(f"File analyzed: {file_path}")
    log_info(f"Total question entries: {summary['total']}")
    log_info(f"Distinct units found: {len(summary['unit_counts'])}")
    log_info(f"Distinct bloom levels found: {len(summary['bloom_counts'])}")

    if summary["missing_fields"]:
        missing_info = ", ".join([f"{field}: {count}" for field, count in summary["missing_fields"].items() if count > 0])
        if missing_info:
            log_info(f"Missing field counts: {missing_info}")

    if summary["options_size_counts"]:
        log_info(f"Options count distribution: {format_counter(summary['options_size_counts'])}")

    if summary["difficulty_counts"]:
        log_info(f"Difficulty distribution: {format_counter(summary['difficulty_counts'])}")
    if summary["status_counts"]:
        log_info(f"Status distribution: {format_counter(summary['status_counts'])}")

    if summary["invalid_options_counts"]:
        log_info(f"Questions with mismatched correct answer label: {summary['invalid_options_counts']}")

    log_info("\nUnit summary:")
    for unit, count in summary["unit_counts"].most_common():
        blooms = summary["bloom_by_unit"].get(unit, {})
        bloom_list = ", ".join([f"{level}: {count}" for level, count in sorted(blooms.items(), key=lambda x: (x[0]))])
        log_info(f"  {unit}: jumlah soal: {count}; distribusi bloom level: {bloom_list if bloom_list else '-'}")

    exact_duplicates = summary["exact_duplicate_groups"]
    duplicate_count = sum(v for v in exact_duplicates.values())
    duplicate_groups = len(exact_duplicates)
    if duplicate_groups:
        log_info(f"Exact duplicate groups: {duplicate_groups}, duplicate entries total: {duplicate_count}")
        sample = list(exact_duplicates.items())[:5]
        for idx, ((question, options), count) in enumerate(sample, start=1):
            log_info(f"  DUPLICATE #{idx}: count={count}, question='{question[:100]}', options={len(options)}")
    else:
        log_info("Exact duplicate groups: 0")

    stem_duplicates = summary["stem_duplicate_groups"]
    stem_duplicate_count = sum(v for v in stem_duplicates.values())
    stem_duplicate_groups = len(stem_duplicates)
    if stem_duplicate_groups:
        log_info(f"Question stem duplicate groups: {stem_duplicate_groups}, repeated stems total: {stem_duplicate_count}")
        sample = list(stem_duplicates.items())[:5]
        for idx, (question, count) in enumerate(sample, start=1):
            log_info(f"  STEM DUPLICATE #{idx}: count={count}, question='{question[:120]}'")
    else:
        log_info("Question stem duplicate groups: 0")

    log_info(f"Bloom-level totals: {format_counter(summary['bloom_counts'])}")


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Check and summarize generated question JSON files."
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Input JSON file path, or filename under data/generated_question/ or data/evaluasi/."
    )
    return parser.parse_args()


def main():
    args = parse_arguments()
    try:
        input_file = resolve_input_file(args.input)
    except FileNotFoundError as exc:
        log_error(str(exc))
        sys.exit(1)

    try:
        entries = load_question_entries(input_file)
    except Exception as exc:
        log_error(f"Failed to read JSON: {exc}")
        sys.exit(1)

    canonical_units = load_canonical_units()
    summary = summarize_generated_questions(entries, canonical_units=canonical_units)
    log_info("=" * 80)
    log_info("GENERATED QUESTION FILE CHECK")
    log_info("=" * 80)
    log_summary(summary, input_file)
    log_info("=" * 80)
    log_info("Analysis completed.")


if __name__ == "__main__":
    main()
