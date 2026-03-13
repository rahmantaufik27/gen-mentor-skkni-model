import json
import random
import os
import subprocess
import datetime

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

# Load data from knowledge_base_fix.json
kb_file = 'data/knowledge_base/knowledge_base_fix.json'
bloom_file = 'data/knowledge_base/bloom.json'

if not os.path.exists(kb_file):
    print(f"❌ Error: {kb_file} not found.")
    exit(1)

if not os.path.exists(bloom_file):
    print(f"❌ Error: {bloom_file} not found.")
    exit(1)

with open(kb_file, encoding='utf-8') as f:
    kb = json.load(f)

with open(bloom_file, encoding='utf-8') as f:
    bloom = json.load(f)

# Distribusi Bloom
bloom_dist = bloom['distribusi']
questions = []

for bloom_level, count in bloom_dist.items():
    for _ in range(count):
        # Pilih unit secara random
        unit = random.choice(kb['unit'])
        unit_kode = unit['kode_unit']
        unit_nama = unit['judul_unit']

        # Cari contoh soal Bloom dari unit
        examples = [e for e in unit['evaluasi'] if e['bloom_level'] == f'C{bloom_level}']
        if not examples:
            # Jika tidak ada, pilih dari unit lain secara random
            other_unit = random.choice([u for u in kb['unit'] if u != unit])
            examples = [e for e in other_unit['evaluasi'] if e['bloom_level'] == f'C{bloom_level}']

        # Ambil contoh random
        if examples:
            example = random.choice(examples)
            contoh_soal = example.get('soal', 'Contoh soal umum')
            contoh_pilihan = example.get('pilihan', ['A. Pilihan A', 'B. Pilihan B'])
            contoh_jawaban = example.get('jawaban', 'A')
        else:
            contoh_soal = "Contoh soal Bloom umum"
            contoh_pilihan = ["A. Jawab A", "B. Jawab B"]
            contoh_jawaban = "A"

        # Pilih konsep random dari unit
        konsep = random.choice(unit['konsep']) if unit['konsep'] else "Konsep umum"

        # Prompt
        prompt = f"Berdasarkan pola soal Bloom berikut dari unit {unit_nama} ({unit_kode}):\n"
        prompt += f"- Contoh Soal {bloom_level}: {contoh_soal}\n  Pilihan: {contoh_pilihan}\n  Jawaban: {contoh_jawaban}\n\n"
        prompt += f"Generate 1 soal pilihan ganda BARU DAN UNIK (tidak sama dengan contoh, gunakan variasi kata-kata) untuk Bloom {bloom_level}, relevan dengan konsep: {konsep}.\nGunakan keywords Bloom: {bloom['bloom_keywords'][bloom_level]}.\nFormat JSON: {{\"question\": \"...\", \"options\": [\"A. ...\", \"B. ...\", \"C. ...\", \"D. ...\"], \"correct_answer\": \"A\", \"bloom_level\": \"{bloom_level}\", \"unit\": \"{unit_kode}\"}}"

        # Panggil Ollama
        response = call_llm_specific("qwen3:4b-instruct", prompt, timeout=120)
        if response:
            # Clean response: remove ```json and ```
            response = response.strip()
            if response.startswith("```json"):
                response = response[7:]
            if response.endswith("```"):
                response = response[:-3]
            response = response.strip()
            try:
                new_question = json.loads(response)
                questions.append(new_question)
            except json.JSONDecodeError:
                print(f"❌ JSON decode error for {bloom_level}: {response[:100]}...")
                # Fallback
                questions.append({
                    "question": f"Contoh soal {bloom_level} untuk unit {unit_kode}",
                    "options": ["A. Jawab A", "B. Jawab B", "C. Jawab C", "D. Jawab D"],
                    "correct_answer": "A",
                    "bloom_level": bloom_level,
                    "unit": unit_kode
                })
        else:
            print(f"❌ No response from Ollama for {bloom_level}")
            # Fallback
            questions.append({
                "question": f"Contoh soal {bloom_level} untuk unit {unit_kode}",
                "options": ["A. Jawab A", "B. Jawab B", "C. Jawab C", "D. Jawab D"],
                "correct_answer": "A",
                "bloom_level": bloom_level,
                "unit": unit_kode
            })

# Simpan output
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
output_file = f'data/generated_question/generated_questions_{timestamp}.json'
with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(questions, f, indent=2, ensure_ascii=False)

print(f"✅ Generated {len(questions)} questions saved to {output_file}")