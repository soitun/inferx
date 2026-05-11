# Sample call with ~/test as input directory
# Config passed as key=value arguments
# API key from environment variable

# Sample call with ~/test as input directory
# Config passed as key=value arguments
# API key from environment variable (set API_KEY in host shell first)

sudo docker run --rm \
  -v /home/brad/test/input:/input \
  -v /home/brad/test/output:/output \
  docling-pdf2md \
    base_url=https://model.inferx.net/funccall/tn-a3t79iogb2/endpoints/Qwen3.6-35B-A3B-FP8/v1 \
    api_key=$API_KEY \
    model=Qwen/Qwen3.6-35B-A3B-FP8

where