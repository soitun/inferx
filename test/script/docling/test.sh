# Sample call with ~/test as input directory
# Config passed as key=value arguments
# API key from environment variable

# Sample call with ~/test as input directory
# Config passed as key=value arguments
# API key from environment variable (set API_KEY in host shell first)

sudo docker run --rm \
  -v ~/test:/input \
  -v /home/brad:/output \
  docling-pdf2md \
    --base-url=https://model.inferx.net/funccall/tn-a3t79iogb2/endpoints/gemma-4-31B-it-fp8 \
    --api-key=$API_KEY \
    --model=google/gemma-4-31B-it

