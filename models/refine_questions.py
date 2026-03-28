#!/usr/bin/env python3
"""
MCQ Quality Refinement Program - LLM-Powered
Uses Ollama Qwen 3.4B Instruct for comprehensive MCQ refinement.

Input: JSON file with generated questions (filename or full path)
Output: JSON file with refined questions in data/evaluasi folder

Refinement Process:
- LLM analyzes each MCQ for quality issues
- LLM provides refinement suggestions and improvements
- LLM determines final status (accepted/revised/rejected)
- Process similar to question generation pipeline
"""

import json
import os
import sys
import subprocess
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


def call_llm_specific(model: str, prompt: str, timeout: int = 120) -> str:
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


class MCQRefiner:
    """LLM-powered MCQ refinement using Ollama Qwen 3.4B Instruct."""
    
    def __init__(self, model="qwen3:4b-instruct", timeout=120):
        """Initialize refiner with Ollama configuration."""
        self.model = model
        self.timeout = timeout
    
    def _extract_json(self, text):
        """Extract JSON from LLM response."""
        try:
            text = text.strip()
            # Remove markdown code blocks if present
            if text.startswith("```json"):
                text = text[7:]
            if text.startswith("```"):
                text = text[3:]
            if text.endswith("```"):
                text = text[:-3]
            text = text.strip()
            
            # Find JSON object
            json_start = text.find("{")
            json_end = text.rfind("}") + 1
            if json_start >= 0 and json_end > json_start:
                json_str = text[json_start:json_end]
                return json.loads(json_str)
        except json.JSONDecodeError as e:
            log_error(f"JSON decode error: {str(e)[:50]}")
            return None
        except Exception as e:
            log_error(f"Error extracting JSON: {str(e)}")
            return None
        
        return None
    
    def refine_question(self, question_dict):
        """Refine a single question using LLM."""
        
        # Extract question components
        bloom_level = question_dict.get("bloom_level", "C3") # if there is not bloom_level, default to C3 for refinement context
        stem = question_dict.get("question", "")  # Using "question" from generated structure
        options = question_dict.get("options", [])  # Using "options" from generated structure
        answer_key = question_dict.get("correct_answer", "")  # Using "correct_answer" from generated structure
        
        # Check for missing/empty fields
        if not stem or not options or not answer_key:
            return {
                "original": question_dict,
                "status": "rejected",
                "reason": "Missing required fields (question, options, or correct_answer)"
            }
        
        # Format options for display
        options_text = "\n".join([
            f"{chr(97+i)}) {opt[:150]}" for i, opt in enumerate(options)
        ])
        
        # Create refinement prompt
        prompt = f"""Anda adalah expert MCQ reviewer, assessment designer, dan quality assurance validator.

TUGAS: Review dan validasi pertanyaan multiple-choice (MCQ) berikut. Deteksi errors, ambiguity, weak distractors, invalid formatting, dan factual risks. Improve MCQ sambil preserving learning objective dan intended difficulty.

BLOOM LEVEL: {bloom_level}

PERTANYAAN (STEM):
{stem}

PILIHAN JAWABAN:
{options_text}

JAWABAN YANG DIKLAIM BENAR:
{answer_key}

KRITERIA VALIDASI STRICT (HARUS dipenuhi semua untuk "accepted"):
1. Stem: clear, self-contained, grammatically correct - tanpa ambiguity atau trick wording
2. Exactly ONE best correct answer (tidak boleh ada competing answers yang equally correct)
3. Distractors: plausible, non-overlapping, dan NOT obviously wrong
4. Question aligned dengan topic dan intended Bloom level {bloom_level}
5. NO duplicate options atau partially correct answers
6. Answer key matches DENGAN PASTI jawaban terbaik
7. Content: age-appropriate, BEBAS dari bias dan offensive wording
8. NO hidden assumptions atau trick wording

ALLOWED ACTIONS (Untuk "revised"):
- Fix grammar, punctuation, wording, clarity, consistency
- Rewrite stem HANYA jika meaning dipreservasi
- Replace weak distractors dengan distractor yang lebih baik dan plausible
- Correct answer key JIKA jawaban terbaik sudah jelas
- Improve explanation jika ada
- Standardize formatting

DISALLOWED ACTIONS:
- JANGAN change learning objective kecuali untuk repair broken item
- JANGAN add unsupported facts beyond source context
- JANGAN create multiple correct answers
- JANGAN return free text outside JSON

DECISION POLICY (STRICT):
- "accepted" = valid as-is ATAU only minimal normalization needed (formatting fixes)
- "revised" = valid AFTER edits (minimal issues fixable)
- "rejected" = CANNOT be made reliable without guessing OR major reconstruction

⚠️ BE STRICT: PREFER rejecting flawed items over silently approving weak ones.
⚠️ Jika ada doubt/ambiguity tentang best answer → REJECT
⚠️ Jika ada multiple equally correct answers → REJECT
⚠️ Jika distractors tiba-tiba obviously wrong → REJECT

Respond HANYA dalam format JSON berikut, TANPA teks tambahan:
{{
  "status": "accepted|revised|rejected",
  "issues": ["issue1", "issue2", ...] atau [],
  "factual_verification_status": "verified|needs_verification|unverifiable",
  "revised_stem": "revised stem OR null",
  "revised_options": ["a) ...", "b) ...", ...] OR null,
  "revised_answer": "revised correct answer OR null",
  "decision_reasoning": "Penjelasan SINGKAT & CLEAR mengapa status ini dipilih. Jika rejected, explain SPECIFICALLY apa yang tidak bisa diperbaiki dan why."
}}"""

        # Call Ollama
        log_info(f"Refining Bloom {bloom_level} question...")
        response_text = call_llm_specific(self.model, prompt, timeout=self.timeout)
        
        if not response_text:
            log_error(f"No response from Ollama for Bloom {bloom_level}")
            return {
                "original": question_dict,
                "status": "error",
                "reason": "LLM API error - unable to process"
            }
        
        # Extract JSON from response
        llm_result = self._extract_json(response_text)
        
        if not llm_result:
            log_error(f"Failed to parse LLM response for Bloom {bloom_level}")
            return {
                "original": question_dict,
                "status": "error",
                "llm_response": response_text[:150]
            }
        
        # Build refined question result
        refined_result = {
            "original": question_dict,
            "status": llm_result.get("status", "error"),
            "issues": llm_result.get("issues", []),
            "factual_verification_status": llm_result.get("factual_verification_status", "unverifiable"),
            "decision_reasoning": llm_result.get("decision_reasoning", llm_result.get("reasoning", "")),
        }
        
        # Add revised fields if they exist
        if llm_result.get("revised_stem"):
            refined_result["revised_stem"] = llm_result["revised_stem"]
        else:
            refined_result["revised_stem"] = None
        
        if llm_result.get("revised_options"):
            refined_result["revised_options"] = llm_result["revised_options"]
        else:
            refined_result["revised_options"] = None
        
        if llm_result.get("revised_answer"):
            refined_result["revised_answer"] = llm_result["revised_answer"]
        else:
            refined_result["revised_answer"] = None
        
        return refined_result


def load_questions(input_file):
    """Load questions from JSON file."""
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        questions = []
        
        # Handle different JSON structures
        if isinstance(data, dict) and "unit" in data:
            # Structure: {"unit": [{"evaluasi": [...]}]}
            for unit in data["unit"]:
                if "evaluasi" in unit:
                    questions.extend(unit["evaluasi"])
        elif isinstance(data, list):
            # Direct list of questions
            questions.extend(data)
        elif isinstance(data, dict) and "questions" in data:
            questions.extend(data["questions"])
        else:
            # Try to extract evaluasi arrays
            for key, value in data.items():
                if isinstance(value, list) and len(value) > 0:
                    if isinstance(value[0], dict) and "soal" in value[0]:
                        questions.extend(value)
        
        log_info(f"Loaded {len(questions)} questions from {input_file}")
        return questions
    
    except Exception as e:
        log_error(f"Failed to load questions: {e}")
        return []


def save_refined_questions(refined_questions, output_file):
    """Save refined questions to JSON file."""
    try:
        # Calculate statistics
        status_counts = {
            "accepted": sum(1 for q in refined_questions if q.get("status") == "accepted"),
            "revised": sum(1 for q in refined_questions if q.get("status") == "revised"),
            "rejected": sum(1 for q in refined_questions if q.get("status") == "rejected"),
            "error": sum(1 for q in refined_questions if q.get("status") == "error")
        }
        
        output_data = {
            "refinement_timestamp": datetime.now().isoformat(),
            "refinement_framework": "LLM-Powered MCQ Refiner (Ollama Qwen 3.4B)",
            "total_questions": len(refined_questions),
            "status_distribution": status_counts,
            "acceptance_rate": f"{status_counts['accepted'] / len(refined_questions) * 100:.1f}%" if refined_questions else "0%",
            "questions": refined_questions
        }
        
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        log_success(f"Saved {len(refined_questions)} refined questions to {output_file}")
        return True
    
    except Exception as e:
        log_error(f"Failed to save refined questions: {e}")
        return False


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="MCQ Quality Refinement Program - Validate and refine multiple-choice questions"
    )
    
    parser.add_argument(
        "--input",
        required=True,
        help="Input JSON file with generated questions (filename or full path)"
    )
    parser.add_argument(
        "--output-dir",
        default="data/evaluasi",
        help="Output directory for refined questions (default: data/evaluasi)"
    )
    
    return parser.parse_args()


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
    log_info("MCQ REFINEMENT PROCESS STARTED")
    log_info("="*70)
    log_info(f"Input file: {input_file}")
    log_info(f"Output directory: {args.output_dir}")
    
    # Load questions
    questions = load_questions(input_file)
    if not questions:
        log_error("No questions loaded from input file")
        sys.exit(1)
    
    log_info(f"Total questions to refine: {len(questions)}\n")
    
    # Initialize refiner
    refiner = MCQRefiner()
    
    # Refine all questions
    refined_questions = []
    
    for i, question in enumerate(questions):
        try:
            bloom_level = question.get("bloom_level", "?")
            log_info(f"[{i+1}/{len(questions)}] Refining Bloom {bloom_level}...")
            
            refined = refiner.refine_question(question)
            refined_questions.append(refined)
            
        except Exception as e:
            log_error(f"Error refining question {i}: {e}")
            refined_questions.append({
                "original": question,
                "status": "error",
                "error": str(e)
            })
    
    log_info("\n" + "="*70)
    log_info("REFINEMENT COMPLETE - SAVING RESULTS")
    log_info("="*70 + "\n")
    
    # Save refined questions
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    input_filename = os.path.splitext(os.path.basename(input_file))[0]
    output_file = os.path.join(
        args.output_dir,
        f"refined_{input_filename}_{timestamp}.json"
    )
    
    if save_refined_questions(refined_questions, output_file):
        # Print summary
        status_counts = {
            "accepted": sum(1 for q in refined_questions if q.get("status") == "accepted"),
            "revised": sum(1 for q in refined_questions if q.get("status") == "revised"),
            "rejected": sum(1 for q in refined_questions if q.get("status") == "rejected"),
            "error": sum(1 for q in refined_questions if q.get("status") == "error")
        }
        
        log_success("\n" + "="*70)
        log_success("REFINEMENT SUMMARY")
        log_success("="*70)
        log_success(f"Total Questions: {len(refined_questions)}")
        log_success(f"✓ Accepted: {status_counts['accepted']} ({status_counts['accepted']/len(refined_questions)*100:.1f}%)")
        log_success(f"◐ Revised:  {status_counts['revised']} ({status_counts['revised']/len(refined_questions)*100:.1f}%)")
        log_success(f"✗ Rejected: {status_counts['rejected']} ({status_counts['rejected']/len(refined_questions)*100:.1f}%)")
        log_success(f"⚠ Error:    {status_counts['error']} ({status_counts['error']/len(refined_questions)*100:.1f}%)")
        log_success(f"\nOutput: {output_file}")
        log_success("="*70 + "\n")
        
        sys.exit(0)
    else:
        log_error("Failed to save refined questions")
        sys.exit(1)


if __name__ == "__main__":
    main()
