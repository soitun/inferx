# Docling PDF-to-Markdown container
# Input: /input/*.pdf  ->  Output: /output/merged.md
FROM python:3.11-slim

# System dependencies for PDF rendering
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    libgl1 \
    libglib2.0-0t64 \
    libgbm1 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libcairo2 \
    libgdk-pixbuf-xlib-2.0-0 \
    libffi-dev \
    fonts-dejavu-core \
    poppler-utils \
    && rm -rf /var/lib/apt/lists/* \
    && fc-cache -f

WORKDIR /app

# Install docling (includes PDF processing backends)
RUN pip install --no-cache-dir docling

# Copy processing script
COPY convert.py ./convert.py

# Create mount points
RUN mkdir -p /input /output

ENTRYPOINT ["python", "-u", "convert.py"]
