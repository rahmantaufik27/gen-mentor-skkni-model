"""
Question Generation Evaluation Program
Evaluates output from question generation strategies using RAGAS framework.

Uses RAGAS library for semantic evaluation and quality assessment against
knowledge_base_fix.json as reference. Implements actual RAGAS metrics:
- Answer Relevancy
- Context Precision
- Context Recall
- Answer Correctness
- Answer Similarity

Usage:
    python models/evaluate_questions.py --input generated_questions_fewshot_perunit_20260327_173827.json
    python models/evaluate_questions.py --input generated_questions_zeroshot_perunit_20260327_180029.json
    python models/evaluate_questions.py --input generated_questions_llm_perunit_20260327_182133.json
"""

import json
import os
import sys
import argparse
from datetime import datetime
from typing import Dict, List, Tuple, Any
import httpx

from ragas import evaluate, Dataset
from ragas.dataset_schema import SingleTurnSample
from datasets import Dataset as HFDataset
from openai import OpenAI
from ragas.llms import llm_factory
from ragas.embeddings import BaseRagasEmbeddings

class OllamaEmbeddings(BaseRagasEmbeddings):
    def __init__(self, model="qwen3:4b-instruct"):
        self.model = model
        self.client = httpx.Client(base_url="http://localhost:11434")

    def embed_query(self, text: str) -> list[float]:
        response = self.client.post(
            "/api/embeddings",
            json={"model": self.model, "prompt": text}
        )
        return response.json()["embedding"]

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        return [self.embed_query(text) for text in texts]

    async def aembed_query(self, text: str) -> list[float]:
        # For simplicity, use sync
        return self.embed_query(text)

    async def aembed_documents(self, texts: list[str]) -> list[list[float]]:
        return [await self.aembed_query(text) for text in texts]
        
# Import RAGAS
try:
    from ragas.metrics import (
        answer_relevancy,
        context_precision,
        context_recall,
        answer_correctness,
        faithfulness,
    )
    RAGAS_AVAILABLE = True
    log_ragas = True
except ImportError:
    RAGAS_AVAILABLE = False
    log_ragas = False
# try:
#     from ragas.metrics.collections import (
#         answer_relevancy,
#         context_precision,
#         context_recall,
#         answer_correctness,
#         semantic_similarity,
#         faithfulness,
#     )
#     from ragas import evaluate
#     from ragas.dataset_schema import SingleTurnSample
#     from openai import OpenAI
#     from ragas.llms import llm_factory
#     from ragas.embeddings import embedding_factory
#     RAGAS_AVAILABLE = True
#     log_ragas = True
# except ImportError:
#     RAGAS_AVAILABLE = False
#     log_ragas = False

def log_info(message: str):
    """Log info message with timestamp."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] INFO: {message}", flush=True)

def log_error(message: str):
    """Log error message with timestamp."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] ERROR: {message}", flush=True)

def log_success(message: str):
    """Log success message with timestamp."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] SUCCESS: {message}", flush=True)

def log_warning(message: str):
    """Log warning message with timestamp."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] WARNING: {message}", flush=True)

# ============================================================
# RAGAS-ONLY EVALUATION
# ============================================================

def evaluate_semantic_with_ragas(question: Dict, kb: Dict) -> Tuple[Dict[str, float], str]:
    """
    Evaluate semantic quality using RAGAS metrics only.
    Returns metrics dictionary and a descriptive reason.
    """
    if not RAGAS_AVAILABLE:
        raise ImportError("RAGAS library is required for evaluation, but it is not available.")

    unit_code = question.get("unit", "")
    question_text = question.get("question", "")
    correct_answer = question.get("correct_answer", "")
    options = question.get("options", [])

    # Find unit concepts in KB. If not found, use empty context list but still run RAGAS.
    contexts = []
    for u in kb.get("unit", []):
        if u.get("kode_unit") == unit_code:
            contexts = u.get("konsep", [])
            break

    answer_map = {"A": 0, "B": 1, "C": 2, "D": 3}
    if correct_answer in answer_map and answer_map[correct_answer] < len(options):
        ground_truth = options[answer_map[correct_answer]]
    else:
        ground_truth = ""

    sample = SingleTurnSample(
        user_input=question_text,
        retrieved_contexts=contexts,
        reference=ground_truth,
        response=ground_truth
    )

    # Initialize local LLM and embeddings using Ollama via OpenAI-compatible API
    client = OpenAI(base_url="http://localhost:11434/v1", api_key="ollama")
    llm = llm_factory("qwen3:4b-instruct", client=client)
    embeddings = OllamaEmbeddings(model="nomic-embed-text:latest")

    metrics = [
        answer_relevancy,
        context_precision,
        context_recall,
        answer_correctness,
        faithfulness,
    ]

    dataset = HFDataset.from_list([sample.to_dict()])

    results = evaluate(dataset, metrics=metrics, llm=llm, embeddings=embeddings)
    df = results.to_pandas()
    ragas_scores = {}

    for metric in metrics:
        metric_name = metric.__class__.__name__
        if metric_name in df.columns:
            ragas_scores[metric_name] = float(df.loc[0, metric_name])
        else:
            ragas_scores[metric_name] = 0.0

    reason_parts = [f"{k}: {v:.3f}" for k, v in ragas_scores.items()]
    reason = "; ".join(reason_parts)

    return ragas_scores, reason

def evaluate_question(question: Dict, kb: Dict) -> Dict:
    """Evaluate a single question using RAGAS-only metrics."""
    ragas_scores, ragas_reason = evaluate_semantic_with_ragas(question, kb)

    metric_values = [v for v in ragas_scores.values() if v is not None]
    overall_score = round(sum(metric_values) / len(metric_values), 3) if metric_values else 0.0

    return {
        "ragas_metrics": ragas_scores,
        "ragas_reason": ragas_reason,
        "overall_quality_score": overall_score
    }

def evaluate_file(file_path: str, kb: Dict, strategy_name: str) -> Dict:
    """Evaluate all questions in a generated file using RAGAS-only metrics."""

    log_info(f"Evaluating {strategy_name} from {file_path}")

    if not os.path.exists(file_path):
        log_error(f"File not found: {file_path}")
        return {"error": f"File not found: {file_path}"}

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            questions = json.load(f)
    except json.JSONDecodeError as e:
        log_error(f"JSON decode error in {file_path}: {e}")
        return {"error": f"JSON decode error: {e}"}

    if not isinstance(questions, list):
        log_error(f"Expected list of questions, got {type(questions)}")
        return {"error": "Expected list of questions"}

    log_info(f"Found {len(questions)} questions to evaluate")

    evaluations = []
    question_scores = []
    ragas_metric_sums = {}

    for idx, q in enumerate(questions, 1):
        eval_result = evaluate_question(q, kb)
        overall = eval_result["overall_quality_score"]
        question_scores.append(overall)

        ragas_scores = eval_result.get("ragas_metrics", {})
        for metric_name, value in ragas_scores.items():
            ragas_metric_sums[metric_name] = ragas_metric_sums.get(metric_name, 0.0) + (value or 0.0)

        evaluations.append({
            "question_number": idx,
            "question_text": q.get("question", "")[:60] + "...",
            "unit": q.get("unit", ""),
            "evaluation": eval_result
        })

    avg_score = round(sum(question_scores) / len(question_scores), 3) if question_scores else 0.0
    min_score = round(min(question_scores), 3) if question_scores else 0.0
    max_score = round(max(question_scores), 3) if question_scores else 0.0

    ragas_metric_averages = {
        metric_name: round(total / len(questions), 3) if questions else 0.0
        for metric_name, total in ragas_metric_sums.items()
    }

    result = {
        "strategy": strategy_name,
        "file": file_path,
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


def generate_evaluation_report(evaluation_results: List[Dict], output_path: str):
    """Generate comprehensive evaluation report using RAGAS metrics."""
    
    log_info(f"Generating comprehensive evaluation report...")
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    report = {
        "evaluation_timestamp": datetime.now().isoformat(),
        "evaluation_framework": "RAGAS-only metrics with AnswerRelevancy, ContextPrecision, ContextRecall, AnswerCorrectness, SemanticSimilarity, Faithfulness",
        "ragas_available": RAGAS_AVAILABLE,
        "total_strategies": len(evaluation_results),
        "strategies": evaluation_results,
        "summary_comparison": {
            "strategy_comparison": [
                {
                    "strategy": ev.get("strategy", ""),
                    "total_questions": ev.get("total_questions", 0),
                    "average_quality_score": ev.get("aggregate_metrics", {}).get("average_quality_score", 0.0),
                    "ragas_metric_averages": ev.get("aggregate_metrics", {}).get("ragas_metric_averages", {})
                }
                for ev in evaluation_results
            ]
        }
    }
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    log_success(f"Report saved to {output_path}")
    
    print(f"\n{'='*80}")
    print(f"EVALUATION SUMMARY (RAGAS-Only)")
    print(f"{'='*80}")
    for ev in evaluation_results:
        print(f"\nStrategy: {ev.get('strategy', 'N/A')}")
        print(f"  Total Questions: {ev.get('total_questions', 0)}")
        print(f"  Average Quality Score: {ev.get('aggregate_metrics', {}).get('average_quality_score', 0.0):.3f}/1.0")
        print(f"  Score Range: {ev.get('aggregate_metrics', {}).get('min_score', 0.0):.3f} - {ev.get('aggregate_metrics', {}).get('max_score', 0.0):.3f}")
        print(f"  RAGAS Averages:")
        for metric, value in ev.get('aggregate_metrics', {}).get('ragas_metric_averages', {}).items():
            print(f"    {metric}: {value:.3f}")

# ============================================================
# ARGUMENT PARSING & MAIN PIPELINE
# ============================================================

def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Evaluate generated questions using RAGAS-based metrics"
    )
    
    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="Input JSON file with generated questions (e.g., generated_questions_fewshot_perunit.json)"
    )
    
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/evaluasi",
        help="Output directory for evaluation report (default: data/evaluasi)"
    )
    
    parser.add_argument(
        "--strategy-name",
        type=str,
        default=None,
        help="Custom strategy name (if not specified, will be inferred from filename)"
    )
    
    return parser.parse_args()

def infer_strategy_name(filename: str) -> str:
    """Infer strategy name from filename."""
    filename = os.path.basename(filename).lower()
    
    if "fewshot" in filename and "perunit" in filename:
        return "Few-Shot RAG Per-Unit"
    elif "zeroshot" in filename and "perunit" in filename:
        return "Zero-Shot RAG Per-Unit"
    elif "llm" in filename and "perunit" in filename:
        return "LLM Direct Per-Unit"
    elif "fewshot" in filename:
        return "Few-Shot RAG"
    elif "zeroshot" in filename:
        return "Zero-Shot RAG"
    elif "llm" in filename:
        return "LLM Direct"
    else:
        return "Unknown Strategy"

def main():
    log_info("="*80)
    log_info("QUESTION GENERATION EVALUATION PROGRAM (RAGAS-Based with Real Metrics)")
    log_info("="*80)
    
    args = parse_arguments()
    
    input_file = args.input
    output_dir = args.output_dir
    strategy_name = args.strategy_name or infer_strategy_name(input_file)
    
    if not os.path.exists(input_file):
        alt_path = os.path.join("data/generated_question", input_file)
        if os.path.exists(alt_path):
            input_file = alt_path
        else:
            log_error(f"Input file not found: {input_file}")
            log_error(f"Tried: {alt_path}")
            sys.exit(1)
    
    log_success(f"Input file: {input_file}")
    log_info(f"Strategy: {strategy_name}")
    
    kb_file = 'data/knowledge_base/knowledge_base_fix.json'
    
    if not os.path.exists(kb_file):
        log_error(f"{kb_file} not found")
        sys.exit(1)
    
    log_info(f"Loading knowledge base from {kb_file}")
    with open(kb_file, encoding='utf-8') as f:
        kb = json.load(f)

    if not RAGAS_AVAILABLE:
        log_error("RAGAS library required but not available. Install ragas first.")
        sys.exit(1)

    log_success("RAGAS library available - running RAGAS-only evaluation")
    
    log_info(f"\n{'='*80}")
    log_info(f"STARTING EVALUATION")
    log_info(f"{'='*80}\n")
    
    result = evaluate_file(input_file, kb, strategy_name)
    
    if "error" in result:
        log_error(f"Evaluation failed: {result['error']}")
        sys.exit(1)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"evaluation_{os.path.splitext(os.path.basename(input_file))[0]}_{timestamp}.json")
    
    generate_evaluation_report([result], output_file)
    
    log_success(f"\n✅ Evaluation complete! Results saved to {output_file}")

if __name__ == "__main__":
    main()
