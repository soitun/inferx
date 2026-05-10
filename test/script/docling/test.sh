sudo docker run --rm \
  -v /home/brad/tmp/pdf:/input \
  -v /tmp/docling_output:/output \
  -v /opt/inferx/cache:/root/.cache/huggingface \
  docling-pdf2md