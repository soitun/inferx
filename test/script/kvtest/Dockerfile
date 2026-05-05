FROM vllm/vllm-openai:v0.19.1

# Dependencies for Revyzor connector
RUN pip install --no-cache-dir \
    cupy-cuda12x \
    nvidia-nvcomp-cu12

# Install production connector (compiled binary)
# Replace the placeholder .so with the real binary from email before building
COPY revyzor_connector_inferx.cpython-312-x86_64-linux-gnu.so /app/revyzor_connector_inferx.cpython-312-x86_64-linux-gnu.so
ENV PYTHONPATH="/app:${PYTHONPATH}"

# Keep test stub for reference
COPY revyzor_test_stub.py /app/revyzor_test_stub.py