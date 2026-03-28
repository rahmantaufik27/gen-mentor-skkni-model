#!/usr/bin/env python3
"""
Refined Questions Evaluation Program
Evaluates refined questions from refine_questions.py output.

Filters questions by status (accepted/revised only) and evaluates using RAGAS-based metrics
with AnswerRelevancy, ContextPrecision, ContextRecall, AnswerCorrectness, AnswerSimilarity.
For "revised" questions, uses the improved stem/options/answer provided by the refiner.

Input: refined_generated_questions_{type}_{timestamp}.json
Output: JSON with RAGAS-based evaluation scores

Usage:
    python models/evaluate_refined_questions.py --input refined_generated_questions_fewshot_perunit_20260327_173827_20260327_221241.json
"""

import json
import os
import sys
import argparse
from datetime import datetime
from typing import Dict, List, Tuple, Any

# Import evaluation functions from evaluate_questions (same RAGAS metrics)
try:
    from evaluate_questions import (
        evaluate_question,
        log_info, log_error, log_success, log_warning,
        RAGAS_AVAILABLE
    )
except ImportError:
    # Fallback: define minimal logging if import fails
    def log_info(msg: str):
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] INFO: {msg}", flush=True)
    
    def log_error(msg: str):
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] ERROR: {msg}", flush=True)
    
    def log_success(msg: str):
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] SUCCESS: {msg}", flush=True)
    
    def log_warning(msg: str):
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] WARNING: {msg}", flush=True)
    
    RAGAS_AVAILABLE = False


def reconstruct_question_from_refined(refined_item: Dict) -> Dict:
    """
    Reconstruct a question object from refined item.
    
    For "revised" status: use revised_stem, revised_options, revised_answer
    For "accepted" status: use original question as-is
    """
    status = refined_item.get("status", "")
    original = refined_item.get("original", {})
    
    if status == "revised":
        # Use revised values if available
        revised_stem = refined_item.get("revised_stem")
        revised_options = refined_item.get("revised_options")
        revised_answer = refined_item.get("revised_answer")
        
        question = {
            "question": revised_stem if revised_stem else original.get("question", ""),
            "options": revised_options if revised_options else original.get("options", []),
            "correct_answer": revised_answer if revised_answer else original.get("correct_answer", ""),
            "bloom_level": original.get("bloom_level", "C3"),
            "unit": original.get("unit", ""),
        }
    elif status == "accepted":
        # Use original as-is
        question = {
            "question": original.get("question", ""),
            "options": original.get("options", []),
            "correct_answer": original.get("correct_answer", ""),
            "bloom_level": original.get("bloom_level", "C3"),
            "unit": original.get("unit", ""),
        }
    else:
        # Unknown status, use original
        question = {
            "question": original.get("question", ""),
            "options": original.get("options", []),
            "correct_answer": original.get("correct_answer", ""),
            "bloom_level": original.get("bloom_level", "C3"),
            "unit": original.get("unit", ""),
        }
    
    return question


def load_refined_questions(input_file: str) -> List[Dict]:
    """
    Load refined questions from JSON file.
    Filter only "accepted" and "revised" status items.
    Reconstruct questions for evaluation.
    """
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        log_error(f"JSON decode error: {e}")
        return []
    except FileNotFoundError:
        log_error(f"File not found: {input_file}")
        return []
    
    # Extract questions array
    questions_data = data.get("questions", [])
    
    # Filter and reconstruct
    filtered_questions = []
    for item in questions_data:
        status = item.get("status", "")
        
        # Only include accepted and revised
        if status in ["accepted", "revised"]:
            question = reconstruct_question_from_refined(item)
            # Add metadata for tracking
            question["_original_status"] = status
            question["_refinement_metadata"] = {
                "status": status,
                "issues": item.get("issues", []),
                "factual_verification_status": item.get("factual_verification_status", ""),
                "decision_reasoning": item.get("decision_reasoning", ""),
            }
            filtered_questions.append(question)
    
    log_info(f"Loaded {len(filtered_questions)} questions (accepted/revised) from {input_file}")
    return filtered_questions


def evaluate_refined_file(
    file_path: str,
    kb: Dict
) -> Dict:
    """
    Evaluate refined questions using RAGAS-based metrics.
    Status (accepted/revised) used only for filtering, not included in output.
    Output structure returns only RAGAS metric details.
    """
    log_info(f"Evaluating refined questions from {file_path}")
    
    # Load refined questions
    questions = load_refined_questions(file_path)
    
    if not questions:
        log_error("No eligible questions found (looking for accepted/revised)")
        return {"error": "No questions to evaluate"}
    
    log_info(f"Evaluating {len(questions)} questions...")
    
    evaluations = []
    question_scores = []
    
    for idx, q in enumerate(questions, 1):
        # Remove metadata fields (status used only for filtering)
        q.pop("_original_status", "")
        q.pop("_refinement_metadata", {})
        
        # Evaluate using RAGAS-only metrics
        eval_result = evaluate_question(q, kb)
        overall = eval_result.get("overall_quality_score", 0.0)
        question_scores.append(overall)
        
        evaluations.append({
            "question_number": idx,
            "question_text": q.get("question", "")[:60] + "..." if q.get("question") else "N/A",
            "unit": q.get("unit", ""),
            "evaluation": eval_result
        })
    
    # Calculate aggregate metrics
    avg_score = round(sum(question_scores) / len(question_scores), 3) if question_scores else 0
    min_score = round(min(question_scores), 3) if question_scores else 0
    max_score = round(max(question_scores), 3) if question_scores else 0
    
    
    ragas_metric_sums = {}
    for eval_result in evaluations:
        ragas = eval_result["evaluation"].get("ragas_metrics", {})
        for metric_name, value in ragas.items():
            ragas_metric_sums[metric_name] = ragas_metric_sums.get(metric_name, 0.0) + (value or 0.0)

    ragas_metric_averages = {
        metric_name: round(total / len(evaluations), 3) if evaluations else 0.0
        for metric_name, total in ragas_metric_sums.items()
    }

    result = {
        "file": file_path,
        "evaluation_type": "Refined Questions (Post-Refinement)",
        "total_questions": len(questions),
        "aggregate_metrics": {
            "average_quality_score": avg_score,
            "min_score": min_score,
            "max_score": max_score,
            "ragas_metric_averages": ragas_metric_averages
        },
        "evaluations": evaluations
    }

    return result


def save_evaluation_report(
    evaluation_result: Dict,
    output_file: str
):
    """Save evaluation report to JSON file."""
    try:
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        report = {
            "evaluation_timestamp": datetime.now().isoformat(),
            "evaluation_framework": "RAGAS-based Semantic + Structural Validation with AnswerRelevancy, ContextPrecision, ContextRecall, AnswerCorrectness, AnswerSimilarity (Refined Questions)",
            "ragas_available": RAGAS_AVAILABLE,
            **evaluation_result
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        log_success(f"Report saved to {output_file}")
        return True
    except Exception as e:
        log_error(f"Failed to save report: {e}")
        return False


def load_knowledge_base(kb_file: str = "data/knowledge_base/knowledge_base_fix.json") -> Dict:
    """Load knowledge base for semantic evaluation."""
    try:
        with open(kb_file, 'r', encoding='utf-8') as f:
            kb = json.load(f)
        log_info(f"Loaded knowledge base from {kb_file}")
        return kb
    except FileNotFoundError:
        log_warning(f"Knowledge base not found: {kb_file}")
        return {"unit": []}
    except json.JSONDecodeError as e:
        log_warning(f"Error loading knowledge base: {e}")
        return {"unit": []}


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Evaluate refined questions using RAGAS-based metrics"
    )
    
    parser.add_argument(
        "--input",
        required=True,
        help="Input refined questions file (refined_generated_questions_*.json)"
    )
    
    parser.add_argument(
        "--output-dir",
        default="data/evaluasi",
        help="Output directory for evaluation report (default: data/evaluasi)"
    )
    
    parser.add_argument(
        "--kb-file",
        default="data/knowledge_base/knowledge_base_fix.json",
        help="Knowledge base file for semantic evaluation"
    )
    
    return parser.parse_args()


def resolve_input_file(input_param: str) -> str:
    """Resolve input file path from various formats."""
    # Check if it's a full path
    if os.path.isfile(input_param):
        return input_param
    
    # Check in data/evaluasi/
    candidate = os.path.join("data", "evaluasi", input_param)
    if os.path.isfile(candidate):
        return candidate
    
    # Check in current directory
    if os.path.isfile(input_param):
        return input_param
    
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
    log_info("REFINED QUESTIONS EVALUATION STARTED")
    log_info("="*70)
    log_info(f"Input: {input_file}")
    log_info(f"Output directory: {args.output_dir}")
    
    # Load knowledge base
    kb = load_knowledge_base(args.kb_file)

    # Evaluate refined questions
    evaluation_result = evaluate_refined_file(input_file, kb)
    
    if "error" in evaluation_result:
        log_error(evaluation_result["error"])
        sys.exit(1)
    
    # Save report
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    input_basename = os.path.splitext(os.path.basename(input_file))[0]
    output_file = os.path.join(
        args.output_dir,
        f"eval_{input_basename}_{timestamp}.json"
    )
    
    if save_evaluation_report(evaluation_result, output_file):
        # Print summary
        metrics = evaluation_result["aggregate_metrics"]
        
        log_success("\n" + "="*70)
        log_success("REFINED QUESTIONS EVALUATION SUMMARY")
        log_success("="*70)
        log_success(f"Total Questions Evaluated: {evaluation_result['total_questions']}")
        log_success(f"\nQuality Metrics:")
        log_success(f"  Average Score: {metrics['average_quality_score']:.3f}/1.0")
        log_success(f"  Score Range: {metrics['min_score']:.3f} - {metrics['max_score']:.3f}")
        log_success(f"  RAGAS Metric Averages: {metrics.get('ragas_metric_averages', {})}")
        log_success(f"\nOutput: {output_file}")
        log_success("="*70 + "\n")
        
        sys.exit(0)
    else:
        log_error("Failed to save evaluation report")
        sys.exit(1)


if __name__ == "__main__":
    main()
