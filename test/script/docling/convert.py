#!/usr/bin/env python3
import sys
import os
from docling.document_converter import DocumentConverter
from pathlib import Path
from datetime import datetime




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

import os

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
    print("  base_url   - LLM endpoint URL", file=sys.stderr)
    print("  api_key    - API key for authentication", file=sys.stderr)
    print("  model      - Model name (e.g., google/gemma-4-31B-it)", file=sys.stderr)
    print("", file=sys.stderr)
    print("Or set API_KEY environment variable.", file=sys.stderr)
    sys.exit(1)

def main():
    input_dir = Path("/input")
    output_dir = Path("/output")
    output_file = output_dir / "merged.md"

    config = parse_args()

    base_url = config.get("base_url")
    api_key_arg = config.get("api_key")
    api_key = get_api_key() if api_key_arg is None or api_key_arg == "" else api_key_arg
    model = config.get("model")

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
    
    docs = convert_files(files, base_url, api_key, model)

    if not docs:
        print("ERROR: No files were successfully converted", file=sys.stderr)
        sys.exit(1)

    markdown = build_markdown(docs)

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(markdown)

    total_chars = len(markdown)
    print(f"\nSuccessfully created {output_file} ({total_chars} chars, {total_chars/1024:.1f} KB)")

if __name__ == "__main__":
    main()
