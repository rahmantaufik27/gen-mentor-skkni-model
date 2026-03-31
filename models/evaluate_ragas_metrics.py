from openai import AsyncOpenAI
from ragas.llms import llm_factory
import load_json
from ragas.metrics.collections import Faithfulness, ContextPrecision, AnswerRelevancy, ContextRecall

# Setup local Ollama via OpenAI-compatible Async client
client_llama = AsyncOpenAI(
    api_key="ollama",
    base_url="http://127.0.0.1:11434/v1",
)

llm = llm_factory(
    model="llama3",
    provider="openai",
    client=client_llama,
)

async def evaluate_generation_faithfulness(
    generated_path: str,
    kb_path: str = "data/knowledge_base/knowledge_base_fix.json",
):
    """Evaluate faithfulness per unit using generated questions and LLM-backed reference."""
    # Load data
    kb_data = load_json(kb_path)
    generated_data = load_json(generated_path)

    def _get_bloom_level(unit: dict) -> str:
        if unit.get("bloom"):
            return unit.get("bloom")
        if unit.get("evaluasi") and len(unit.get("evaluasi", [])) > 0:
            return unit["evaluasi"][0].get("bloom_level", "C1")
        return "C1"

    class LlamaReferenceModel(__import__("pydantic").BaseModel):
        text: str

    for unit in kb_data.get("unit", []):
        unit_id = unit.get("kode_unit", "unknown_unit")
        bloom_level = _get_bloom_level(unit)

        # Filter generated questions for this unit
        unit_questions = [q for q in generated_data if q.get("unit") == unit_id]
        if not unit_questions:
            continue

        # Prepare evaluation inputs
        user_input = (
            f"Generate 1 soal pilihan ganda BARU DAN UNIK, soal harus relevan dengan konsep di unit "
            f"{unit_id} dengan tingkat kesulitan {bloom_level}."
        )

        # Response: all generated questions as one big response
        response = "\n\n".join(
            [
                f"Question: {q.get('question')}\nOptions: {', '.join(q.get('options', []))}\nCorrect: {q.get('correct_answer')}"
                for q in unit_questions
            ]
        )

        # Retrieved contexts: KB concepts (for RAGAS retrieval context)
        retrieved_contexts = unit.get("konsep", [])

        # Reference: from LLM's own knowledge, not the local KB directly
        reference = ""
        try:
            reference_prompt = (
                f"Berdasarkan pengetahuan internal model Llama, berikan ringkasan referensi singkat untuk unit {unit_id} "
                f"dengan tingkat kesulitan {bloom_level} dan konsep utama: {', '.join(retrieved_contexts[:3])}. "
                "Format jawab: satu paragraf pendek."
            )
            llm_ref = await llm.agenerate(reference_prompt, response_model=LlamaReferenceModel)
            reference = getattr(llm_ref, "text", str(llm_ref))
        except Exception:
            # fallback jika pemanggilan LLM gagal
            reference = " ".join(retrieved_contexts[:3]) if retrieved_contexts else ""

        # Evaluate Faithfulness
        scorer = Faithfulness(llm=llm)
        result = await scorer.ascore(
            user_input=user_input,
            response=response,
            retrieved_contexts=retrieved_contexts,
        )

        # Evaluate Context Recall (coverage of model-knowledge reference)
        recall_scorer = ContextRecall(llm=llm)
        recall_result = await recall_scorer.ascore(
            user_input=user_input,
            retrieved_contexts=retrieved_contexts,
            reference=reference,
        )

        print(
            f"Unit {unit_id}: bloom_level={bloom_level}, Faithfulness={result.value:.2f}, "
            f"Recall={recall_result.value:.2f}"
        )

async def faithfull_metric():

    # Create metric
    scorer = Faithfulness(llm=llm)

    # Evaluate
    result = await scorer.ascore(
        user_input="When was the first super bowl?",
        response="The first superbowl was held on Jan 15, 1967",
        retrieved_contexts=[
            "The First AFL-NFL World Championship Game was an American football game played on January 15, 1967, at the Los Angeles Memorial Coliseum in Los Angeles."
        ]
    )
    print(f"Faithfulness Score: {result.value}")

async def contextpre_metric():
    # Create metric
    scorer = ContextPrecision(llm=llm)

    # Evaluate
    result = await scorer.ascore(
        user_input="Where is the Eiffel Tower located?",
        reference="The Eiffel Tower is located in Paris.",
        retrieved_contexts=[
            "The Eiffel Tower is located in Paris.",
            "The Brandenburg Gate is located in Berlin."
        ]
    )
    print(f"Context Precision Score: {result.value}")

# def arelevancy_metric():
#     embeddings = embedding_factory("openai", model="text-embedding-3-small", client=client)
#     # Create metric
#     scorer = AnswerRelevancy(llm=llm, embeddings=embeddings)

#     # Evaluate
#     result = scorer.ascore(
#         user_input="When was the first super bowl?",
#         response="The first superbowl was held on Jan 15, 1967"
#     )
#     print(f"Answer Relevancy Score: {result.value}")

async def contextrec_metric():
    # Create metric
    scorer = ContextRecall(llm=llm)

    # Evaluate
    # result = await scorer.ascore(
    result = await scorer.ascore(
        user_input="Where is the Eiffel Tower located?",
        retrieved_contexts=["Paris is the capital of France."],
        reference="The Eiffel Tower is located in Paris."
    )
    print(f"Context Recall Score: {result.value}")

if __name__ == "__main__":
    import argparse
    import asyncio

    parser = argparse.ArgumentParser(description="Evaluate RAGAS metrics with unit-level bloom and external generated file.")
    parser.add_argument("--generated", required=False, help="Path to generated questions JSON file")
    parser.add_argument("--kb", default="data/knowledge_base/knowledge_base_fix.json", help="Path to knowledge base JSON file")
    parser.add_argument("--mode", choices=["faithfulness", "quick"], default="faithfulness", help="Mode: faithfulness uses evaluate_generation_faithfulness; quick runs sample static metrics")
    args = parser.parse_args()

    if args.mode == "faithfulness":
        if not args.generated:
            raise ValueError("--generated path is required for faithfulness mode")
        asyncio.run(evaluate_generation_faithfulness(args.generated, args.kb))
    else:
        asyncio.run(faithfull_metric())
        asyncio.run(contextpre_metric())
        asyncio.run(contextrec_metric())