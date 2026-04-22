import json
import os
import sys
import argparse
from datetime import datetime

def log_info(message: str):
    """Log info message with timestamp"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] INFO: {message}", flush=True)

def log_error(message: str):
    """Log error message with timestamp"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] ERROR: {message}", flush=True)

def log_success(message: str):
    """Log success message with timestamp"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] SUCCESS: {message}", flush=True)

def extract_accepted_revised_questions(input_file_path, output_file_path):
    """
    Extract questions that are either 'accepted' or 'revised' from the refined generated questions JSON file.
    For each question, extract: question, options, correct_answer, bloom_level, unit.
    For 'accepted' status, use data from 'original'.
    For 'revised' status, use data from 'revised_*' fields if available, otherwise fall back to 'original'.
    """
    if not os.path.exists(input_file_path):
        log_error(f"Input file {input_file_path} does not exist.")
        return

    with open(input_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    extracted_questions = []

    for item in data.get('questions', []):
        status = item.get('status')
        if status in ['accepted', 'revised']:
            # Determine source of data
            if status == 'accepted':
                question = item.get('original', {}).get('question')
                options = item.get('original', {}).get('options')
                correct_answer = item.get('original', {}).get('correct_answer')
                bloom_level = item.get('original', {}).get('bloom_level')
                unit = item.get('original', {}).get('unit')
            elif status == 'revised':
                question = item.get('revised_stem') or item.get('original', {}).get('question')
                options = item.get('revised_options') or item.get('original', {}).get('options')
                correct_answer = item.get('revised_answer') or item.get('original', {}).get('correct_answer')
                bloom_level = item.get('original', {}).get('bloom_level')
                unit = item.get('original', {}).get('unit')

            # Only include if all required fields are present
            if question and options and correct_answer and bloom_level and unit:
                extracted_questions.append({
                    'question': question,
                    'options': options,
                    'correct_answer': correct_answer,
                    'bloom_level': bloom_level,
                    'unit': unit
                })

    # Write to output file
    os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
    with open(output_file_path, 'w', encoding='utf-8') as f:
        json.dump(extracted_questions, f, ensure_ascii=False, indent=4)

    log_success(f"Extracted {len(extracted_questions)} questions to {output_file_path}")

def resolve_input_file(input_param):
    """Resolve input file path from various formats."""
    # Check if it's a full path
    if os.path.isfile(input_param):
        return input_param
    
    # Check in current directory
    if os.path.isfile(input_param):
        return input_param
    
    # Check in data/generated_question/
    candidate = os.path.join("data", "generated_question", input_param)
    if os.path.isfile(candidate):
        return candidate
    
    # Check in data/evaluasi/ (for previously refined files)
    candidate = os.path.join("data", "evaluasi", input_param)
    if os.path.isfile(candidate):
        return candidate
    
    raise FileNotFoundError(f"Input file not found: {input_param}")

def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Extract accepted and revised questions from refined MCQ JSON file"
    )
    
    parser.add_argument(
        "--input",
        required=True,
        help="Input JSON file with refined questions (filename or full path)"
    )
    
    return parser.parse_args()

def main():
    """Main execution."""
    args = parse_arguments()
    
    # Resolve input file
    try:
        input_file = resolve_input_file(args.input)
    except FileNotFoundError as e:
        log_error(str(e))
        sys.exit(1)
    
    log_info("="*70)
    log_info("EXTRACTION PROCESS STARTED")
    log_info("="*70)
    log_info(f"Input file: {input_file}")
    
    # Generate output file path
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    input_filename = os.path.splitext(os.path.basename(input_file))[0]
    output_file = os.path.join(
        "data", "generated_question",
        f"extracted_{input_filename}_{timestamp}.json"
    )
    
    log_info(f"Output file: {output_file}")
    
    # Extract questions
    extract_accepted_revised_questions(input_file, output_file)

if __name__ == "__main__":
    main()