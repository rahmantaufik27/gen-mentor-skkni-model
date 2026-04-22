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

def call_llm_specific(model: str, prompt: str, timeout: int = 120) -> str:
    """Run a prompt on a specific Ollama model."""
    import subprocess
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

class MCQEvaluator:
    """LLM-powered MCQ evaluation using Ollama Qwen 3.4B Instruct."""

    def __init__(self, model="qwen3:4b-instruct", timeout=120):
        """Initialize evaluator with Ollama configuration."""
        self.model = model
        self.timeout = timeout

    def evaluate_question(self, question_dict, question_number):
        """Evaluate a single question using LLM."""

        # Extract question components
        question = question_dict.get("question", "")
        options = question_dict.get("options", [])
        correct_answer = question_dict.get("correct_answer", "")
        bloom_level = question_dict.get("bloom_level", "")
        unit = question_dict.get("unit", "")

        # Format options for display
        options_text = "\n".join([
            f"{chr(97+i)}) {opt}" for i, opt in enumerate(options)
        ])

        # Create evaluation prompt
        prompt = f"""You are an expert educational assessment reviewer acting as an LLM judge for multiple-choice questions (MCQs).
            Your task is to evaluate the final quality of each MCQ using a rubric-based framework that approximates the review process of instructional designers or subject-matter experts.
            Framework purpose:
            - Assess whether the item is aligned with the intended competency unit or learning objective.
            - Assess whether the item is linguistically clear and structurally readable.
            - Assess whether the actual cognitive demand required by the item matches the assigned Bloom taxonomy level.
            You must evaluate each item using exactly three criteria:
            1. Relevance (R)
            Definition: alignment with the intended competency unit or learning objective
            Scale:
            5 = Perfectly aligned; directly measures the stated competency/unit with no irrelevant content
            4 = Mostly aligned; minor looseness in focus, but still clearly relevant
            3 = Partially aligned; some connection exists, but the item does not cleanly target the competency
            2 = Weak alignment; only indirect or superficial connection to the competency
            1 = Not aligned; does not meaningfully assess the stated competency
            2. Clarity (C)
            Definition: linguistic comprehensibility and structural readability
            Scale:
            5 = Very clear, concise, grammatically correct, self-contained, and easy to interpret
            4 = Mostly clear; minor wording or structural issues, but still easy to answer
            3 = Understandable but somewhat awkward, wordy, vague, or structurally uneven
            2 = Difficult to understand due to wording, ambiguity, grammar, or poor structure
            1 = Unclear or confusing to the point that interpretation is unreliable
            3. Cognitive Depth (D)
            Definition: consistency between the reasoning required and the assigned
            Bloom level
            Scale:
            5 = Cognitive demand precisely matches the assigned Bloom level
            4 = Mostly matches the Bloom level, with only a slight mismatch
            3 = Partially matches; the question mixes levels or is somewhat misaligned
            2 = Clear mismatch; actual thinking required is noticeably different from the assigned level
            1 = Severe mismatch; the assigned Bloom level is incorrect for the task required
            Bloom alignment guide:
            - C1 Remember: recall, recognize, identify, name, list, define
            - C2 Understand: explain, summarize, classify, compare, interpret
            - C3 Apply: use, execute, implement, solve in a familiar situation
            - C4 Analyze: differentiate, organize, infer, examine relationships
            - C5 Evaluate: judge, justify, critique, defend
            - C6 Create: design, construct, generate, produce
            Judgment instructions:
            - Evaluate the actual MCQ, not only the competency label.
            - Judge each criterion independently.
            - Use only integer scores from 1 to 5.
            - If uncertain between two scores, choose the lower score unless there is clear evidence for the higher score.
            - Be consistent across all items in the batch.
            - Use score 5 only when the criterion is clearly strong.
            - Use score 3 for adequate but imperfect quality.
            - Lower Cognitive Depth when the actual reasoning demand does not match the assigned Bloom level.
            - Lower Clarity when the question is unnecessarily wordy, grammatically awkward, vague, or structurally confusing.
            - Lower Relevance when the item only loosely or indirectly targets the stated competency.
            Feedback instructions:
            - Provide concise professional feedback in 1 to 3 sentences in English with a concise, academic, but cohesive and coherent manner.
            - Avoid words overused by AI and dash - mark.
            - Mention the main strength and the main weakness.
            - Briefly comment on Bloom alignment.
            - Avoid generic comments.
            Output instructions:
            - Return only a JSON object.
            - Use exactly these keys: no, question, r, c, d, feedback
            - Preserve the numbering and question text from the input.
            - Do not add any explanation outside the JSON.
            Now evaluate these items:
            {question_number}. Question: {question}
            Options:
            {options_text}
            Correct Answer: {correct_answer}
            Bloom Level: {bloom_level}
            Unit: {unit}
        """

        # Call Ollama
        log_info(f"Evaluating question {question_number}...")
        response_text = call_llm_specific(self.model, prompt, timeout=self.timeout)

        if not response_text:
            log_error(f"No response from Ollama for question {question_number}")
            return None

        # Parse the JSON from response
        try:
            result = json.loads(response_text)
            return result
        except json.JSONDecodeError as e:
            log_error(f"Failed to parse JSON for question {question_number}: {e}")
            return None

def load_questions(input_file):
    """Load questions from JSON file."""
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except Exception as e:
        log_error(f"Failed to load questions: {e}")
        return []

def save_evaluation_json(evaluations, output_file):
    """Save the evaluations to a JSON file."""
    try:
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(evaluations, f, indent=2, ensure_ascii=False)
        log_success(f"Saved {len(evaluations)} evaluations to {output_file}")
        return True
    except Exception as e:
        log_error(f"Failed to save evaluations: {e}")
        return False

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
        description="Evaluate MCQ quality using RCD framework"
    )
    
    parser.add_argument(
        "--input",
        required=True,
        help="Input JSON file with questions to evaluate (filename or full path)"
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
    log_info("MCQ EVALUATION PROCESS STARTED")
    log_info("="*70)
    log_info(f"Input file: {input_file}")
    
    # Load questions
    questions = load_questions(input_file)
    if not questions:
        log_error("No questions loaded from input file")
        sys.exit(1)
    
    log_info(f"Total questions to evaluate: {len(questions)}\n")
    
    # Initialize evaluator
    evaluator = MCQEvaluator()
    
    # Evaluate all questions
    evaluations = []
    
    for i, question in enumerate(questions):
        try:
            log_info(f"[{i+1}/{len(questions)}] Evaluating...")
            
            result = evaluator.evaluate_question(question, i+1)
            if result:
                evaluations.append(result)
            else:
                # Add a placeholder if evaluation failed
                evaluations.append({
                    "no": i+1,
                    "question": question.get("question", "N/A"),
                    "r": "N/A",
                    "c": "N/A",
                    "d": "N/A",
                    "feedback": "Evaluation failed"
                })
            
        except Exception as e:
            log_error(f"Error evaluating question {i+1}: {e}")
            evaluations.append({
                "no": i+1,
                "question": question.get("question", "N/A"),
                "r": "N/A",
                "c": "N/A",
                "d": "N/A",
                "feedback": f"Error: {str(e)[:50]}"
            })
    
    log_info("\n" + "="*70)
    log_info("EVALUATION COMPLETE - SAVING RESULTS")
    log_info("="*70 + "\n")
    
    # Save evaluation table
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    input_filename = os.path.splitext(os.path.basename(input_file))[0]
    output_file = os.path.join(
        "data", "evaluasi",
        f"evaluated_rcd_{input_filename}_{timestamp}.json"
    )
    
    if save_evaluation_json(evaluations, output_file):
        log_success(f"\nOutput: {output_file}")
        log_success("="*70 + "\n")
        
        sys.exit(0)
    else:
        log_error("Failed to save evaluations")
        sys.exit(1)

if __name__ == "__main__":
    main()