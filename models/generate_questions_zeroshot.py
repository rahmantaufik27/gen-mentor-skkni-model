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
            log_error(f"Ollama error (exit code {proc.returncode}): {proc.stderr[:100]}")
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

# Load data from knowledge_base_fix.json
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
    bloom = json.load(f)

# Distribusi Bloom
bloom_dist = bloom['distribusi']
questions = []
total_questions = sum(bloom_dist.values())

log_info(f"Starting generation of {total_questions} questions (Zero-Shot RAG approach)")
log_info(f"Bloom distribution: {bloom_dist}")

question_count = 0

for bloom_level, count in bloom_dist.items():
    log_info(f"Generating {count} questions for Bloom level C{bloom_level}")
    
    for i in range(count):
        question_count += 1
        
        # Pilih unit secara random
        unit = random.choice(kb['unit'])
        unit_kode = unit['kode_unit']
        unit_nama = unit['judul_unit']

        # Pilih konsep random dari unit (TANPA contoh soal)
        konsep = random.choice(unit['konsep']) if unit['konsep'] else "Konsep umum"

        # Prompt: Zero-Shot RAG - hanya memberikan konteks konsep, TANPA contoh soal
        prompt = f"""Buatkan 1 soal pilihan ganda untuk tingkat Bloom C{bloom_level}.

Unit: {unit_nama} ({unit_kode})

Konteks Konsep:
{konsep}

Persyaratan:
- Soal harus relevan dengan konsep di atas
- Tingkat kesulitan: {bloom['bloom_keywords'][bloom_level]}
- Format jawaban: JSON dengan struktur yang tepat
- Jangan gunakan contoh soal yang sudah ada, buat soal yang orisinal dan unik

Format output JSON:
{{"question": "...", "options": ["A. ...", "B. ...", "C. ...", "D. ..."], "correct_answer": "A", "bloom_level": "C{bloom_level}", "unit": "{unit_kode}"}}"""

        # Panggil Ollama
        log_info(f"[Q{question_count}/{total_questions}] Calling Ollama for C{bloom_level} on unit {unit_kode}...")
        response = call_llm_specific("qwen3:4b-instruct", prompt, timeout=120)
        
        if response:
            # Clean response: remove ```json and ```
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
                questions.append(new_question)
                log_success(f"[Q{question_count}/{total_questions}] Generated question for C{bloom_level}")
            except json.JSONDecodeError as e:
                log_error(f"JSON decode error for C{bloom_level}: {str(e)[:80]}")
                # Fallback
                fallback_q = {
                    "question": f"Contoh soal C{bloom_level} untuk unit {unit_kode}",
                    "options": ["A. Pilihan A", "B. Pilihan B", "C. Pilihan C", "D. Pilihan D"],
                    "correct_answer": "A",
                    "bloom_level": f"C{bloom_level}",
                    "unit": unit_kode
                }
                questions.append(fallback_q)
                log_error(f"[Q{question_count}/{total_questions}] Using fallback for C{bloom_level}")
        else:
            log_error(f"No response from Ollama for C{bloom_level}")
            # Fallback
            fallback_q = {
                "question": f"Contoh soal C{bloom_level} untuk unit {unit_kode}",
                "options": ["A. Pilihan A", "B. Pilihan B", "C. Pilihan C", "D. Pilihan D"],
                "correct_answer": "A",
                "bloom_level": f"C{bloom_level}",
                "unit": unit_kode
            }
            questions.append(fallback_q)
            log_error(f"[Q{question_count}/{total_questions}] Using fallback for C{bloom_level}")

# Simpan output
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
output_file = f'data/generated_question/generated_questions_zeroshot_{timestamp}.json'

# Pastikan direktori output ada
os.makedirs(os.path.dirname(output_file), exist_ok=True)

with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(questions, f, indent=2, ensure_ascii=False)

log_success(f"Generated {len(questions)} questions saved to {output_file}")
print(f"✅ File: {output_file}")
