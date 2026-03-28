#!/usr/bin/env python3
"""Quick test of LLM connection."""

import subprocess
import json
import sys

def call_llm_test(model, prompt, timeout=30):
    """Test LLM call."""
    cmd = [
        'ollama', 'run', model,
        '--format', 'json'
    ]
    
    try:
        result = subprocess.run(
            cmd,
            input=prompt.encode('utf-8'),
            capture_output=True,
            timeout=timeout,
            text=False
        )
        
        return result.stdout.decode('utf-8', errors='replace')
    except subprocess.TimeoutExpired:
        print(f"ERROR: Timeout after {timeout}s")
        return None
    except Exception as e:
        print(f"ERROR: {str(e)}")
        return None

# Test with simple prompt
test_prompt = """Respond with JSON only:
{"test": "working"}"""

print("[Testing] Calling Ollama qwen3:4b-instruct...")
result = call_llm_test("qwen3:4b-instruct", test_prompt, timeout=30)

if result:
    print(f"[Success] Got response (first 200 chars):\n{result[:200]}")
else:
    print("[Failed] No response or timeout")
