import asyncio
import sys
import os
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from src.utils.translator import NewsTranslator

async def main():
    print("=== AI News Agent: Key Health Check ===")
    translator = NewsTranslator()
    
    if not translator.all_keys:
        print("ERROR: No keys found in settings. Check your .env file.")
        return

    print(f"Detected {len(translator.openai_keys)} OpenAI keys and {len(translator.groq_keys)} Groq keys.")
    print("Testing keys now (this may take a minute)...")
    
    report = await translator.verify_all_keys()
    
    print("\n=== FINAL REPORT ===")
    print(f"ACTIVE:  {len(report['active'])}")
    for k in report['active']:
        print(f"  - {k}")
        
    print(f"\nLIMITED: {len(report['limited'])}")
    for k in report['limited']:
        print(f"  - {k}")
        
    print(f"\nDEAD:    {len(report['dead'])}")
    for k in report['dead']:
        print(f"  - {k}")

    print("\n=== SYSTEM HEALTH ===")
    if not report['active']:
        print("CRITICAL: No active keys! Translation will fail.")
    elif len(report['active']) < 3:
        print("WARNING: Low key redundancy. Recommend adding more keys.")
    else:
        print("STABLE: Sufficient key redundancy for high concurrency.")

if __name__ == "__main__":
    asyncio.run(main())
