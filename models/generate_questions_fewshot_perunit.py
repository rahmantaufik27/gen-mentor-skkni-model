import json
import random
import os
import subprocess
import datetime
import sys

def log_info(message: str):
    """Log info message with timestamp"""
    timestamp = datetime.datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] INFO: {message}", flush=True)

def log_error(message: str):
    """Log error message with timestamp"""
    timestamp = datetime.datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] ERROR: {message}", flush=True)

def log_success(message: str):
    """Log success message with timestamp"""
    timestamp = datetime.datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] SUCCESS: {message}", flush=True)

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
        if proc.returncode != 0:
            log_error(f"Ollama error (exit code {proc.returncode})")
            return ""
        return proc.stdout.strip()
    except subprocess.TimeoutExpired:
        log_error(f"Timeout after {timeout}s")
        return ""
    except FileNotFoundError:
        log_error("Ollama not found. Make sure it's installed and in PATH.")
        sys.exit(1)
    except Exception as e:
        log_error(f"Unexpected error: {str(e)}")
        return ""

# Load data from knowledge_base_fix.json and bloom.json
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

# Validate bloom levels
bloom_levels = ['C1', 'C2', 'C3', 'C4', 'C5', 'C6']
total_per_unit = sum(bloom_dist.get(level, 0) for level in bloom_levels)

log_info(f"Bloom distribution per unit: {bloom_dist}")
log_info(f"Questions per unit: {total_per_unit}")

all_questions = []
unit_count = len(kb['unit'])

log_info(f"Starting generation of {total_per_unit * unit_count} questions ({unit_count} units × {total_per_unit} soal)")
log_info("Strategy: FEW-SHOT RAG LLM (with example questions from KB)")

# ============================================================
# MAIN LOOP: PER UNIT (1-6)
# ============================================================
for unit_idx, unit in enumerate(kb['unit'], 1):
    unit_kode = unit['kode_unit']
    unit_nama = unit['judul_unit']
    
    log_info(f"\n{'='*70}")
    log_info(f"UNIT {unit_idx}/6: {unit_nama} ({unit_kode})")
    log_info(f"Generating {total_per_unit} questions for this unit")
    log_info(f"{'='*70}")
    
    unit_questions = []
    question_num = 0
    
    # ============================================================
    # INNER LOOP: PER BLOOM LEVEL (C1-C6)
    # ============================================================
    for bloom_level in bloom_levels:
        count = bloom_dist.get(bloom_level, 0)
        
        if count == 0:
            continue
        
        log_info(f"Generating {count} questions for Bloom level {bloom_level}")
        
        for i in range(count):
            question_num += 1
            
            # Pick a random concept from this unit
            konsep = random.choice(unit['konsep']) if unit['konsep'] else "Konsep umum"
            
            # Find example questions with this bloom_level from unit (this is a few-shot approach, so we want to provide an example)
            examples = [e for e in unit['evaluasi'] if e['bloom_level'] == bloom_level]
            
            if not examples:
                # If no examples in this unit, search in other units
                other_units = [u for u in kb['unit'] if u != unit]
                for other_unit in other_units:
                    examples = [e for e in other_unit['evaluasi'] if e['bloom_level'] == bloom_level]
                    if examples:
                        break
            
            # Use example if found
            if examples:
                example = random.choice(examples)
                contoh_soal = example.get('soal', 'Contoh soal umum')
                contoh_pilihan = example.get('pilihan', ['A. Pilihan A', 'B. Pilihan B'])
                contoh_jawaban = example.get('jawaban', 'A')
            else:
                contoh_soal = f"Contoh soal Bloom {bloom_level} umum"
                contoh_pilihan = ["A. Jawab A", "B. Jawab B", "C. Jawab C", "D. Jawab D"]
                contoh_jawaban = "A"
            
            # FEW-SHOT Prompt: Include example question from unit
            prompt = f"""Unit: {unit_nama} ({unit_kode})

                Konteks Konsep dari Unit ini:
                {konsep}

                Contoh Soal Bloom {bloom_level} dari Unit yang sama:
                {contoh_soal}
                Pilihan: {contoh_pilihan}
                Jawaban: {contoh_jawaban}

                Instruksi:
                Generate 1 soal pilihan ganda BARU DAN UNIK (jangan sama dengan contoh, gunakan variasi lain) untuk Bloom level {bloom_level}.
                - Soal harus relevan dengan konsep di unit ini
                - Tingkat kesulitan: {", ".join(bloom_keywords[bloom_level][:3])}
                - Format jawaban: JSON

                Format output (HANYA JSON, tanpa teks lain):
                {{"question": "...", "options": ["A. ...", "B. ...", "C. ...", "D. ..."], "correct_answer": "A", "bloom_level": "{bloom_level}", "unit": "{unit_kode}"}}
            """

            # Call Ollama
            log_info(f"  [{question_num}/{total_per_unit}] Generating {bloom_level} question...")
            response = call_llm_specific("qwen3:4b-instruct", prompt, timeout=120)
            
            if response:
                # Clean response
                response = response.strip()
                if response.startswith("```json"):
                    response = response[7:]
                if response.startswith("```"):
                    response = response[3:]
                if response.endswith("```"):
                    response = response[:-3]
                response = response.strip()
                
                try:
                    new_question = json.loads(response)
                    unit_questions.append(new_question)
                    log_success(f"  [{question_num}/{total_per_unit}] {bloom_level} question generated")
                except json.JSONDecodeError as e:
                    log_error(f"  [{question_num}/{total_per_unit}] JSON decode error: {str(e)[:50]}")
                    # Fallback
                    fallback_q = {
                        "question": f"Soal {bloom_level} dari unit {unit_kode}",
                        "options": ["A. Pilihan A", "B. Pilihan B", "C. Pilihan C", "D. Pilihan D"],
                        "correct_answer": "A",
                        "bloom_level": bloom_level,
                        "unit": unit_kode
                    }
                    unit_questions.append(fallback_q)
                    log_error(f"  [{question_num}/{total_per_unit}] Using FALLBACK")
            else:
                log_error(f"  [{question_num}/{total_per_unit}] No response from Ollama")
                # Fallback
                fallback_q = {
                    "question": f"Soal {bloom_level} dari unit {unit_kode}",
                    "options": ["A. Pilihan A", "B. Pilihan B", "C. Pilihan C", "D. Pilihan D"],
                    "correct_answer": "A",
                    "bloom_level": bloom_level,
                    "unit": unit_kode
                }
                unit_questions.append(fallback_q)
                log_error(f"  [{question_num}/{total_per_unit}] Using FALLBACK")
    
    log_success(f"Completed {len(unit_questions)} questions for unit {unit_idx}")
    all_questions.extend(unit_questions)

# ============================================================
# SAVE ALL QUESTIONS TO SINGLE FILE
# ============================================================
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
output_file = f'data/generated_question/generated_questions_fewshot_perunit_{timestamp}.json'

# Ensure output directory exists
os.makedirs(os.path.dirname(output_file), exist_ok=True)

with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(all_questions, f, indent=2, ensure_ascii=False)

log_success(f"\n{'='*70}")
log_success(f"GENERATION COMPLETE!")
log_success(f"{'='*70}")
log_success(f"Total questions generated: {len(all_questions)}")
log_success(f"Output file: {output_file}")
print(f"✅ File saved: {output_file}")
