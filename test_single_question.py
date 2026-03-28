#!/usr/bin/env python3
"""Test refinement on a single question."""

import sys
import os
sys.path.insert(0, os.path.join(os.getcwd(), 'models'))

from refine_questions import MCQRefiner
import json

# Sample question from the generated file
test_question = {
    "question": "Dalam sistem interaktif, bentuk interaksi yang memungkinkan pengguna memberikan input melalui bahasa alami seperti 'cari informasi tentang kota Jakarta' adalah contoh dari jenis interaksi apa?",
    "options": [
        "A. Command Language Interaction",
        "B. Menu GUI Interaction",
        "C. Natural Language Interaction",
        "D. Object-based Interaction"
    ],
    "correct_answer": "C",
    "bloom_level": "C1",
    "unit": "J.620100.005.02"
}

print("[Testing] Refinement of single question...")
refiner = MCQRefiner()
result = refiner.refine_question(test_question)

print("\n[Result]")
print(json.dumps(result, indent=2, ensure_ascii=False))
