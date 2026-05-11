#!/usr/bin/env python3
import sys
import os
from docling.document_converter import DocumentConverter
from pathlib import Path
from datetime import datetime
from typing import Optional
import time

def get_input_files(input_dir: Path):
    """Get all compatible files from input directory and subdirectories."""
    supported_ext = [".pdf", ".docx", ".pptx", ".html", ".htm", ".md", ".txt"]
    files = []
    for item in input_dir.rglob("*"):
        if item.is_file() and item.suffix.lower() in supported_ext:
            files.append(item)
    return sorted(files)

def convert_files(files: list[Path], base_url: str, api_key: str, model: str) -> list[dict]:
    """Convert all files using docling with remote VLM."""
    os.environ["DOCLING_API_URL"] = base_url
    os.environ["DOCLING_API_KEY"] = api_key
    os.environ["DOCLING_API_MODEL"] = model
    os.environ["DOCLING_ENABLE_VISION"] = "true"
    
    converter = DocumentConverter()
    
    converted_docs = []

    for i, file_path in enumerate(files, 1):
        print(f"[{i}/{len(files)}] Processing: {file_path.name}")
        try:
            result = converter.convert(file_path)
            content = result.document.export_to_markdown()
            converted_docs.append({
                "name": file_path.name,
                "content": content,
                "size_mb": file_path.stat().st_size / (1024 * 1024),
                "path": str(file_path)
            })
            print(f"  -> {len(content)} chars extracted")
        except Exception as e:
            print(f"  ERROR: {e}", file=sys.stderr)

    return converted_docs

def generate_summary(docs: list[dict]) -> str:
    """Generate a summary/index for all documents."""
    summary = []
    summary.append("# Document Knowledge Base\n")
    summary.append("---\n\n")
    summary.append("## Summary\n\n")
    summary.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
    summary.append(f"**Total Documents:** {len(docs)}\n\n")
    
    total_size = sum(d["size_mb"] for d in docs)
    summary.append(f"**Total Size:** {total_size:.2f} MB\n\n")
    
    summary.append("---\n\n")
    summary.append("## Table of Contents\n\n")
    
    for i, doc in enumerate(docs, 1):
        summary.append(f"{i}. [{doc['name']}](#{doc['name'].replace('.', '_').lower()})\n")
    
    summary.append("\n---\n\n")
    summary.append("## Documents\n\n")
    
    return "".join(summary)

def build_markdown(docs: list[dict]) -> str:
    """Build merged markdown with summary and clear document boundaries."""
    markdown = generate_summary(docs)
    
    for doc in docs:
        markdown += f"### document: `{doc['name']}`\n\n"
        markdown += doc["content"]
        markdown += "\n\n" + "=" * 72 + "\n\n"
    
    return markdown

def parse_args():
    """Parse key=value arguments for config."""
    config = {}
    for arg in sys.argv[1:]:
        if "=" in arg:
            key, value = arg.split("=", 1)
            key = key.lstrip("-").replace("-", "_")
            if value != "":
                config[key] = value
    return config

def get_api_key() -> str:
    """Get API key from environment."""
    return os.environ.get("API_KEY", "")

def use_info():
    """Print usage information."""
    print("Usage: docling-pdf2md base_url=... api_key=... model=...", file=sys.stderr)
    print("", file=sys.stderr)
    print("Required config arguments:", file=sys.stderr)
    print("  base_url   - LLM endpoint URL (e.g., https://model.inferx.net/.../v1)", file=sys.stderr)
    print("  api_key    - API key for authentication", file=sys.stderr)
    print("  model      - Model name without provider (e.g., Qwen3-Coder-Next-FP8)", file=sys.stderr)
    print("", file=sys.stderr)
    print("Or set API_KEY environment variable.", file=sys.stderr)
    sys.exit(1)

def main():
    input_dir = Path("/input")
    output_dir = Path("/output")
    docling_output = output_dir / "merged.md"
    dspy_output = output_dir / "optimized.md"

    config = parse_args()

    base_url = config.get("base_url", "https://model.inferx.net/funccall/tn-a3t79iogb2/endpoints/Qwen3.6-35B-A3B-FP8/v1")
    api_key_arg = config.get("api_key")
    api_key = get_api_key() if api_key_arg is None or api_key_arg == "" else api_key_arg
    model = config.get("model", "Qwen/Qwen3.6-35B-A3B-FP8")

    if not base_url or not api_key or not model:
        use_info()

    output_dir.mkdir(parents=True, exist_ok=True)

    if not input_dir.exists():
        print(f"ERROR: Input directory {input_dir} does not exist", file=sys.stderr)
        sys.exit(1)

    files = get_input_files(input_dir)
    if not files:
        print("ERROR: No compatible files found in /input", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(files)} files to convert.")
    print(f"Using model: {model}")
    print(f"Base URL: {base_url}")
    
    # Docling processing
    docling_start = time.time()
    docs = convert_files(files, base_url, api_key, model)
    docling_end = time.time()
    
    if not docs:
        print("ERROR: No files were successfully converted", file=sys.stderr)
        sys.exit(1)

    markdown = build_markdown(docs)

    with open(docling_output, "w", encoding="utf-8") as f:
        f.write(markdown)

    docling_chars = len(markdown)
    print(f"\nDocling: Created {docling_output} ({docling_chars} chars, {docling_chars/1024:.1f} KB)")
    print(f"Docling processing time: {docling_end - docling_start:.2f} seconds")

    # DSPy optimization
    dspy_start = time.time()
    print("\nOptimizing with DSPy...")
    try:
        import dspy
        from dspy.signatures import Signature
        
        lm = dspy.LM(f"openai/{model}", 
                     api_base=base_url, 
                     api_key=api_key, 
                     max_tokens=20000, 
                     stop=None, 
                     temperature=0,
                     cache=False)
        dspy.configure(lm=lm)
        
        print(f"  DSPy {dspy.__version__} using LLM backend: {model}")
        
        class OptimizeMarkdown(Signature):
            """Optimize markdown content for KV caching by reducing redundancy while preserving structure."""
            raw_markdown: str = dspy.InputField(desc="Original markdown content to optimize")
            optimized_markdown: str = dspy.OutputField(desc="Optimized markdown with reduced redundancy")
        
        optimizer = dspy.Predict(OptimizeMarkdown)
        
        print(f"  Optimizing {len(markdown)} chars of markdown...")
        result = optimizer(raw_markdown=markdown)
        optimized = result.optimized_markdown
        
        dspy_end = time.time()
        optimized_chars = len(optimized)
        reduction = (1 - optimized_chars / len(markdown)) * 100 if len(markdown) > 0 else 0
        
        with open(dspy_output, "w", encoding="utf-8") as f:
            f.write(optimized)
        
        print(f"  DSPy: Created {dspy_output}")
        print(f"    Original: {len(markdown)} chars ({len(markdown)/1024:.1f} KB)")
        print(f"    Optimized: {optimized_chars} chars ({optimized_chars/1024:.1f} KB)")
        print(f"    Reduction: {reduction:.1f}%")
        print(f"    Processing time: {dspy_end - dspy_start:.2f} seconds")
        
    except ImportError:
        print("WARNING: DSPy not available. Skipping optimization.")
    except Exception as e:
        print(f"ERROR in DSPy optimization: {e}")
        import traceback
        traceback.print_exc()

    print(f"\n{'='*60}")
    print("PROCESSING SUMMARY")
    print(f"{'='*60}")
    print(f"Docling processing time: {docling_end - docling_start:.2f} seconds")
    print(f"  Input files: {len(files)}")
    print(f"  Docling output: {docling_chars} chars")
    if 'dspy_end' in locals():
        print(f"\nDSPy processing time: {dspy_end - dspy_start:.2f} seconds")
        print(f"  Optimized output: {optimized_chars} chars")
    print(f"\nOutput files:")
    print(f"  - {docling_output} (original merged content)")
    print(f"  - {dspy_output} (optimized for KV caching)")

if __name__ == "__main__":
    main()
