#!/usr/bin/env bash
set -euo pipefail

MODEL_REVISION="7e3e67edbbed1bf9888184d9df282b700a323964"
MODEL_URL="https://huggingface.co/HuggingFaceTB/SmolVLM-256M-Instruct/resolve/${MODEL_REVISION}"
DESTINATION="${1:-${APPVIEW_ALT_MODEL_DIR:-}}"

if [[ -z "$DESTINATION" ]]; then
  echo "usage: $0 MODEL_DIRECTORY (or set APPVIEW_ALT_MODEL_DIR)" >&2
  exit 2
fi

MODEL_DIRECTORY="$DESTINATION/alt-caption-v1"
mkdir -p "$MODEL_DIRECTORY"

verify_sha256() {
  local expected="$1"
  local file="$2"
  local actual
  if command -v sha256sum >/dev/null 2>&1; then
    actual="$(sha256sum "$file" | awk '{print $1}')"
  else
    actual="$(shasum -a 256 "$file" | awk '{print $1}')"
  fi
  [[ "$actual" == "$expected" ]]
}

fetch_file() {
  local expected="$1"
  local relative_path="$2"
  local destination="$MODEL_DIRECTORY/$relative_path"
  local temporary="$destination.part.$$"
  mkdir -p "$(dirname "$destination")"
  if [[ -f "$destination" ]] && verify_sha256 "$expected" "$destination"; then
    printf 'verified %s\n' "$relative_path"
    return
  fi
  rm -f "$destination"
  if ! curl --fail --location --retry 3 --silent --show-error \
    "$MODEL_URL/$relative_path?download=true" --output "$temporary"; then
    rm -f "$temporary"
    return 1
  fi
  if ! verify_sha256 "$expected" "$temporary"; then
    rm -f "$temporary"
    echo "SHA-256 mismatch for $relative_path at revision $MODEL_REVISION" >&2
    return 1
  fi
  mv "$temporary" "$destination"
  printf 'downloaded %s\n' "$relative_path"
}

fetch_file 74135b8664b56088c0006f1c8e848d79a8eba003411f72ebf1dc2ee96227be3a added_tokens.json
fetch_file a68ad1a42681ae44eacd109ff8dd56a840f761c03d72f4ff4c515d092f882168 chat_template.json
fetch_file b70fb4bfde88df9eeebc9d8ff523733b8bf70d6c9b06c610325960e06ae9db52 config.json
fetch_file 067a2a54e5f87162ecac6e0e911cc4665fc8f7f3324794ecbac0f76badb56636 generation_config.json
fetch_file 0b54e8aa4e53d5383e2e4bc635a56b43f9647f7b13832d5d9ecd8f82dac4f510 merges.txt
fetch_file 306fad6cec3b5b7613600b74abf1e410fa50000fbc81292c0d083bd8d85b5fe2 onnx/decoder_model_merged_int8.onnx
fetch_file 4b919b829fe2bf225e42431230c56891116f0149e5170e36c99bcc0cfae0d2ef onnx/embed_tokens_int8.onnx
fetch_file d534ab668d3563e6d1d809bbbf1ff18308ded48651178614dbcc4a1de13d16f7 onnx/vision_encoder_int8.onnx
fetch_file 6cb6e36d6fcb88ca1502c4a26750715dc3e7dedddc9a8f17b27d8d167d1457e7 preprocessor_config.json
fetch_file e7bff42da73ae9eec9042ef20e066e11f1ee20f025358ff79131e3c0fb549b46 processor_config.json
fetch_file aa0ff906077086dfa9734a7f97f68c825877a48f9468807be65504495cdeef09 special_tokens_map.json
fetch_file 36c6fd44d07d10fd8180ee6b46dcccf69fb7c06753968ff0d7e17b8bfe17b777 tokenizer_config.json
fetch_file 5ece781dc8d2b2f3e2f289ca0ae50b17cfc27dd27bfe7971bb8241e0b964331a tokenizer.json
fetch_file 82b84012e3add4d01d12ba14442026e49b8cbbaead1f79ecf3d919784f82dc79 vocab.json

echo "Model installed at $MODEL_DIRECTORY (14 files, 264713119 bytes)."
