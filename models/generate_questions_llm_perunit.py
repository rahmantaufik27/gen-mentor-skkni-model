import json
import subprocess
import sys
import os
import random
from datetime import datetime

def log_info(message: str):
    """Print info message with timestamp."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] INFO: {message}", flush=True)
    sys.stdout.flush()

def log_error(message: str):
    """Print error message with timestamp."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] ERROR: {message}", flush=True)
    sys.stderr.flush()

def log_success(message: str):
    """Print success message with timestamp."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] SUCCESS: {message}", flush=True)
    sys.stdout.flush()

def call_llm_specific(model: str, prompt: str, timeout: int = 120) -> str:
    """Run a prompt on a specific Ollama model.

    Uses UTF-8 decoding and replaces invalid bytes to avoid
    `UnicodeDecodeError` when reading subprocess output on Windows.
    
    Args:
        model: Ollama model name (e.g., 'qwen3:4b-instruct')
        prompt: The prompt to send to the model
        timeout: Timeout in seconds (default 120 = 2 minutes per question)
    
    Returns:
        Model response as string
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
        
        if proc.returncode != 0:
            log_error(f"Ollama returned error code {proc.returncode}")
            return ""
        
        return proc.stdout.strip()
        
    except subprocess.TimeoutExpired:
        log_error(f"Ollama call timed out after {timeout}s")
        return ""
    except FileNotFoundError:
        log_error("Ollama not found. Make sure it's installed and in PATH.")
        return ""
    except Exception as e:
        log_error(f"Unexpected error: {str(e)}")
        return ""

def generate_questions():
    """Generate questions per unit using per-unit prompt strategy."""
    log_info("Starting question generation process (Per-Unit Strategy)...")
    
    # Load knowledge base and bloom distribution
    kb_file = 'data/knowledge_base/knowledge_base_fix.json'
    bloom_file = 'data/knowledge_base/bloom.json'
    
    if not os.path.exists(kb_file):
        log_error(f"{kb_file} not found.")
        sys.exit(1)
    
    if not os.path.exists(bloom_file):
        log_error(f"{bloom_file} not found.")
        sys.exit(1)
    
    log_info(f"Loading knowledge base from {kb_file}")
    with open(kb_file, encoding='utf-8') as f:
        kb = json.load(f)
    
    log_info(f"Loading Bloom distribution from {bloom_file}")
    with open(bloom_file, encoding='utf-8') as f:
        bloom_data = json.load(f)
    
    bloom_dist = bloom_data['distribusi']
    bloom_keywords = bloom_data['bloom_keywords']
    
    # Bloom levels
    bloom_levels = ['C1', 'C2', 'C3', 'C4', 'C5', 'C6']
    total_per_unit = sum(bloom_dist.get(level, 0) for level in bloom_levels)
    
    log_info(f"Bloom distribution per unit: {bloom_dist}")
    log_info(f"Questions per unit: {total_per_unit}")
    
    model_name = "qwen3:4b-instruct"
    all_questions = []
    unit_count = len(kb['unit'])
    
    log_info(f"Total units: {unit_count}")
    log_info(f"Total questions to generate: {total_per_unit * unit_count}")
    
    # ============================================================
    # MAIN LOOP: PER UNIT
    # ============================================================
    for unit_idx, unit in enumerate(kb['unit'], 1):
        unit_kode = unit['kode_unit']
        unit_nama = unit['judul_unit']
        
        log_info(f"\n{'='*80}")
        log_info(f"UNIT {unit_idx}/{unit_count}: {unit_nama}")
        log_info(f"Kode Unit: {unit_kode}")
        log_info(f"Generating {total_per_unit} questions for this unit")
        log_info(f"{'='*80}")
        
        # Build bloom distribution details for prompt
        bloom_details = []
        for bloom_level in bloom_levels:
            count = bloom_dist.get(bloom_level, 0)
            if count > 0:
                keywords = ", ".join(bloom_keywords.get(bloom_level, [])[:2])
                bloom_details.append(f"- {bloom_level}: {count} soal ({keywords})")
        
        bloom_details_str = "\n".join(bloom_details)
        
        # Create prompt for this unit
        prompt = f"""You are an expert item writer for software competency certification exams. Your task is to generate exactly 10 multiple-choice questions in Indonesian language for the following competency unit.

            UNIT: {unit_nama}
            UNIT CODE: {unit_kode}

            BLOOM'S TAXONOMY DISTRIBUTION FOR THIS UNIT (Total: 10 questions):
            {bloom_details_str}

            GENERAL INSTRUCTIONS:
            1. Generate EXACTLY {total_per_unit} questions total for this unit, adhering strictly to the Bloom distribution above.
            2. Each question must be specifically relevant to this unit's concepts.
            3. Write clear, concise questions in Bahasa Indonesia.
            4. Each question must have exactly 4 answer choices (A, B, C, D).
            5. Only one correct answer per question.
            6. Distractors must be plausible and in the same category.
            7. Follow Bloom's Taxonomy levels:
            - C1 (Remember): Recall facts and basic concepts
            - C2 (Understand): Understand meaning and concepts
            - C3 (Apply): Apply knowledge in new situations
            - C4 (Analyze): Analyze relationships and structures
            - C5 (Evaluate): Justify decisions and evaluate information
            - C6 (Create): Create new solutions and designs
            8. Avoid ambiguity, double negatives, and compound questions.
            9. Do NOT include explanations, commentary, or any text outside JSON.

            OUTPUT FORMAT:
            Return a JSON ARRAY containing exactly {total_per_unit} objects with this structure:
            {{
            "question": "...",
            "options": ["A. ...", "B. ...", "C. ...", "D. ..."],
            "correct_answer": "A",
            "bloom_level": "C1-C6",
            "unit": "{unit_kode}"
            }}

            IMPORTANT: Output ONLY the JSON array, nothing else. Start with [ and end with ].
        """

        log_info(f"Calling Ollama for unit {unit_idx}...")
        response = call_llm_specific(model_name, prompt, timeout=120)
        
        if not response:
            log_error(f"No response from Ollama for unit {unit_idx}")
            # Use fallback questions
            for bloom_level in bloom_levels:
                count = bloom_dist.get(bloom_level, 0)
                for i in range(count):
                    fallback_q = {
                        "question": f"Soal {bloom_level} dari unit {unit_kode}",
                        "options": ["A. Pilihan A", "B. Pilihan B", "C. Pilihan C", "D. Pilihan D"],
                        "correct_answer": "A",
                        "bloom_level": bloom_level,
                        "unit": unit_kode
                    }
                    all_questions.append(fallback_q)
            log_error(f"Unit {unit_idx}: Using {total_per_unit} fallback questions")
            continue
        
        # Parse JSON from response
        try:
            # Extract JSON array
            start_idx = response.find('[')
            end_idx = response.rfind(']')
            
            if start_idx == -1 or end_idx == -1:
                log_error(f"No JSON array found in response for unit {unit_idx}")
                # Use fallback
                for bloom_level in bloom_levels:
                    count = bloom_dist.get(bloom_level, 0)
                    for i in range(count):
                        fallback_q = {
                            "question": f"Soal {bloom_level} dari unit {unit_kode}",
                            "options": ["A. Pilihan A", "B. Pilihan B", "C. Pilihan C", "D. Pilihan D"],
                            "correct_answer": "A",
                            "bloom_level": bloom_level,
                            "unit": unit_kode
                        }
                        all_questions.append(fallback_q)
                log_error(f"Unit {unit_idx}: Using {total_per_unit} fallback questions")
                continue
            
            json_str = response[start_idx:end_idx+1]
            unit_questions = json.loads(json_str)
            
            if not isinstance(unit_questions, list):
                log_error(f"Expected JSON array, got {type(unit_questions).__name__}")
                raise ValueError("Invalid JSON structure")
            
            if len(unit_questions) != total_per_unit:
                log_error(f"Expected {total_per_unit} questions, got {len(unit_questions)}")
                # Still use what we got
            
            # Validate and process questions
            valid_count = 0
            for q in unit_questions:
                try:
                    required_keys = ["question", "options", "correct_answer", "bloom_level"]
                    if all(key in q for key in required_keys):
                        if isinstance(q.get("options"), list) and len(q.get("options", [])) == 4:
                            if q.get("bloom_level") in bloom_levels:
                                q["unit"] = unit_kode
                                all_questions.append(q)
                                valid_count += 1
                except Exception as e:
                    log_error(f"Error processing question: {str(e)}")
            
            if valid_count > 0:
                log_success(f"Unit {unit_idx}: Successfully generated {valid_count}/{total_per_unit} questions")
            else:
                log_error(f"Unit {unit_idx}: No valid questions generated")
                # Use fallback
                for bloom_level in bloom_levels:
                    count = bloom_dist.get(bloom_level, 0)
                    for i in range(count):
                        fallback_q = {
                            "question": f"Soal {bloom_level} dari unit {unit_kode}",
                            "options": ["A. Pilihan A", "B. Pilihan B", "C. Pilihan C", "D. Pilihan D"],
                            "correct_answer": "A",
                            "bloom_level": bloom_level,
                            "unit": unit_kode
                        }
                        all_questions.append(fallback_q)
        
        except json.JSONDecodeError as e:
            log_error(f"JSON decode error in unit {unit_idx}: {e.msg}")
            # Use fallback
            for bloom_level in bloom_levels:
                count = bloom_dist.get(bloom_level, 0)
                for i in range(count):
                    fallback_q = {
                        "question": f"Soal {bloom_level} dari unit {unit_kode}",
                        "options": ["A. Pilihan A", "B. Pilihan B", "C. Pilihan C", "D. Pilihan D"],
                        "correct_answer": "A",
                        "bloom_level": bloom_level,
                        "unit": unit_kode
                    }
                    all_questions.append(fallback_q)
            log_error(f"Unit {unit_idx}: Using fallback questions due to JSON error")
    
    log_info(f"\n{'='*80}")
    log_success(f"FINAL RESULTS")
    log_info(f"{'='*80}")
    log_success(f"Total questions generated: {len(all_questions)}")
    
    # Summary statistics
    if all_questions:
        unit_counts = {}
        bloom_counts = {}
        
        for q in all_questions:
            unit = q.get("unit", "Unknown")
            bloom = q.get("bloom_level", "Unknown")
            unit_counts[unit] = unit_counts.get(unit, 0) + 1
            bloom_counts[bloom] = bloom_counts.get(bloom, 0) + 1
        
        log_info("Unit distribution:")
        for unit_kode in sorted(unit_counts.keys()):
            unit_name = next((u['judul_unit'] for u in kb['unit'] if u['kode_unit'] == unit_kode), "Unknown")
            count = unit_counts[unit_kode]
            log_info(f"  {unit_kode}: {count} questions")
        
        log_info("Bloom level distribution:")
        for bloom in ['C1', 'C2', 'C3', 'C4', 'C5', 'C6']:
            count = bloom_counts.get(bloom, 0)
            log_info(f"  {bloom}: {count} questions")
    
    return all_questions

if __name__ == "__main__":
    log_info("="*80)
    log_info("QUESTION GENERATION PROCESS - PER UNIT STRATEGY")
    log_info("="*80)
    
    try:
        questions = generate_questions()
        
        # Save to file with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"data/generated_question/generated_questions_llm_perunit_{timestamp}.json"
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        log_info(f"Saving questions to: {output_path}")
        
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(questions, f, ensure_ascii=False, indent=2)
        
        log_success(f"Questions generated and saved to {output_path}")
        log_success(f"Total: {len(questions)} questions")
        log_info("="*80)
        
    except Exception as e:
        log_error(f"Process failed: {str(e)}")
        log_info("="*80)
        sys.exit(1)
