#!/usr/bin/env python3
import sys
from docling.document_converter import DocumentConverter
from pathlib import Path

def get_input_files(input_dir: Path):
    """Get all compatible files from input directory."""
    supported_ext = [".pdf", ".docx", ".pptx", ".html", ".htm", ".md", ".txt"]
    files = sorted([f for f in input_dir.iterdir()
                    if f.suffix.lower() in supported_ext and f.is_file()])
    return files

def convert_files(files: list[Path]) -> list[dict]:
    """Convert all files using docling."""
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
            })
            print(f"  -> {len(content)} chars extracted")
        except Exception as e:
            print(f"  ERROR: {e}", file=sys.stderr)

    return converted_docs

def build_markdown(docs: list[dict]) -> str:
    """Build merged markdown with TOC and clear document boundaries."""
    sections = []
    sections.append("# Merged Document Knowledge Base\n")
    sections.append("---\n\n")
    sections.append("## Table of Contents\n\n")

    for i, doc in enumerate(docs, 1):
        anchor = doc["name"].replace(".", "_").lower()
        sections.append(f"{i}. [{doc['name']}](#{anchor})\n")

    sections.append("\n---\n\n")
    sections.append("## Documents\n\n")

    for doc in docs:
        sections.append(f"### document: `{doc['name']}`\n\n")
        sections.append(doc["content"])
        sections.append("\n\n" + "=" * 72 + "\n\n")

    return "".join(sections)

def main():
    input_dir = Path("/input")
    output_dir = Path("/output")
    output_file = output_dir / "merged.md"

    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    if not input_dir.exists():
        print(f"ERROR: Input directory {input_dir} does not exist", file=sys.stderr)
        sys.exit(1)

    files = get_input_files(input_dir)
    if not files:
        print("ERROR: No compatible files found in /input", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(files)} files to convert.")
    docs = convert_files(files)

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
