# Docling PDF to Markdown Converter

Convert PDF, DOCX, PPTX, HTML, MD, and TXT files to optimized markdown using Docling with remote LLM (Qwen3-Coder-Next-FP8 via inferx).

## Purpose

This tool converts document files to markdown format using Docling's remote VLM backend, then optimizes the output for KV cache efficiency in LLM processing. It supports:

1. **Docling LLM processing**: Uses remote Qwen3-Coder-Next-FP8 model instead of local CPU
2. **LLM-optimized format**: System instructions integrated for direct LLM prompt usage
3. **DSPy optimization** (optional): Compresses markdown using LLM-based whitespace optimization
4. **Lossless compression** (default/fallback): Safe whitespace normalization without content changes
5. **Batch processing**: Recursively processes all files in an input directory

## When to Use

- Convert scanned PDFs to searchable markdown
- Prepare documents for RAG/KV cache optimization
- Merge multiple documents into a single knowledge base
- Reduce token count for downstream LLM processing
- Create LLM-ready prompts with system instructions

## Setup

### Prerequisites

- Docker (with sudo access or user in docker group)
- Input files in `/home/brad/test/input/` directory

### Build Docker Image

```bash
cd /home/brad/rust/inferx/test/script/docling
sudo docker build -t docling-test .
```

## Usage

### Basic Usage (Lossless Compression)

```bash
sudo docker run --rm \
  -v /home/brad/test/input:/input \
  -v /home/brad/test/output:/output \
   -e "API_KEY=YOUR_API_KEY_HERE" \
   docling-test \
   base_url=https://model.inferx.net/funccall/tn-a3t79iogb2/endpoints/Qwen3-Coder-Next-FP8/v1 \
   api_key=YOUR_API_KEY_HERE \
   model=Qwen/Qwen3-Coder-Next-FP8
```

### With DSPy Optimization (Experimental)

```bash
sudo docker run --rm \
  -v /home/brad/test/input:/input \
  -v /home/brad/test/output:/output \
   -e "API_KEY=YOUR_API_KEY_HERE" \
   -e "USE_DSPY=true" \
   docling-test \
   base_url=https://model.inferx.net/funccall/tn-a3t79iogb2/endpoints/Qwen3-Coder-Next-FP8/v1 \
   api_key=YOUR_API_KEY_HERE \
   model=Qwen/Qwen3-Coder-Next-FP8
```

## Output Files

- `/home/brad/test/output/merged.md`: Original Docling output with summary and document boundaries
- `/home/brad/test/output/optimized.md`: DSPy or lossless compressed version (KV cache optimized)
- `/home/brad/test/output/llm.md`: LLM-ready format with system instructions for LLM prompts

### LLM-Optimized Output Format (`llm.md`)

```
## SYSTEM INSTRUCTION
You are analyzing technical documents. Follow these rules:

1. **Grounding**: Only use information from the documents below
2. **Missing content**: If formulas appear as placeholders, infer meaning from surrounding text
3. **Images**: Treat diagram descriptions as contextual hints
4. **Code**: Explain code blocks when relevant
5. **Citations**: Always cite both filename AND section number

   **Correct examples:**
   - `[bitcoin.pdf, Section 4 - Proof-of-Work]`
   - `[bitcoin.pdf, Section 11]`
   - `[bitcoin.pdf, Section 5, Step 3]`

   **Incorrect examples:**
   - `[bitcoin.pdf]` (missing section)
   - `Section 4` (missing filename)

## DOCUMENTS

## Document: [filename]
[document content]

## QUESTION
[User's question here]
```

**Features:**
- Ready-to-use system instruction for LLM prompts
- Citation format guidance with examples
- Diagrams replaced with contextual descriptions
- Section headers preserved for easy reference

## Configuration

### Arguments (passed to container)

- `base_url` - LLM endpoint URL (default: inferx endpoint)
- `api_key` - API key for authentication
- `model` - Model name without provider prefix (default: `Qwen/Qwen3-Coder-Next-FP8`)

### Environment Variables

- `API_KEY` - Alternative way to provide API key
- `USE_DSPY=true` - Enable DSPy optimization (disabled by default)

## Supported File Types

- PDF (.pdf)
- Word (.docx)
- PowerPoint (.pptx)
- HTML (.html, .htm)
- Markdown (.md)
- Text (.txt)

All files in `/input` and subdirectories are processed recursively.

## Processing Time

- **Docling**: ~10 seconds per file (uses remote LLM + OCR)
- **Lossless compression**: ~0.00 seconds (fast)
- **DSPy optimization**: ~40-150 seconds (chunked, depends on file size)
- **LLM optimization**: Built-in (no extra time - part of Docling output)

## Notes

- DSPy is **experimental** and may occasionally corrupt output (validation fallback to original)
- Lossless compression is **safe and deterministic** but provides minimal token savings (~1-2%)
- DSPy can achieve ~10-20% reduction with the same content
- OCR models are pre-downloaded in Docker image
