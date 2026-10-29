# Lane av27: on-device alt-text suggestions

Status: implemented. Nothing is downloaded until the user taps **Suggest alt text** (or starts `/alt-test`). Captions run in a same-origin worker; the selected image is passed to that worker as a `File` and is not sent in a request. Posting remains independent of caption generation.

## Model choice and measurements

I compared two pinned ONNX exports in Firefox 153.0.3 headless on this Mac, using the three authored fixture PNGs and single-thread WASM. These are desktop WASM results, not iPhone measurements.

| Candidate | License and pinned revision | Model payload | WASM result and fixture quality |
| --- | --- | ---: | --- |
| **SmolVLM-256M-Instruct (selected)** | Apache-2.0, [`7e3e67edbbed1bf9888184d9df282b700a323964`](https://huggingface.co/HuggingFaceTB/SmolVLM-256M-Instruct/tree/7e3e67edbbed1bf9888184d9df282b700a323964) | **264,713,119 bytes**, 14 files | Loads in about 0.9–1.1 s. Captions: cat 81.1 s, bicycle 80.2 s, dog 80.8 s. It names the cat and bicycle, but invents airplane details for the bicycle; the dog caption only says “an animal in the middle.” |
| Florence-2-base-ft | MIT, [`e88a44eaf3791a35eae0c5a47b3dbcd36e67eb6f`](https://huggingface.co/onnx-community/Florence-2-base-ft/tree/e88a44eaf3791a35eae0c5a47b3dbcd36e67eb6f), using `embed_tokens:int8`, `encoder_model:q4`, `decoder_model_merged:q4`, `vision_encoder:bnb4` | 209,796,456 bytes of ONNX weights; 214,025,711 bytes including metadata | Loads in about 1.0 s. Captions took 12.9 s, 13.5 s, and 15.6 s. It calls the cat a dog, gets the bicycle subject, and describes the water scene but misses the dog. |

Both model payloads fit the 300 MB target. SmolVLM was selected because its captions identified the cat and bicycle subjects more reliably; Florence was faster and smaller, but misidentified the cat. Neither model is consistently accurate, so every suggestion remains optional and editable. The bicycle hallucination and vague dog caption are recorded here as known quality limits.

Observed captions:

- SmolVLM: cat — “In this image there is a cartoon image of a cat on the green background.”; bicycle — “This is a logo which is in the shape of a bicycle. The bicycle is red and black in color. The wings are orange in color. The image is in the shape of a plane.”; dog — “In this image there is an animal in the middle of the image.”
- Florence-2: cat — “A cartoon image of a dog laying in a bed. The dog is brown and has two pointed ears. The bed is blue and the ground is green.”; bicycle — “A picture of a red bicycle. There are two large wheels on the front of the bike. There is a small yellow button on the bottom of the bicycle.”; dog — “A cartoon image of a brown animal is on a beach. The animal has a round nose and two small black eyes. The background of the image is a light blue color. There are waves in the water behind the animal. The ground beneath the animal is light brown.”

SmolVLM first-use feature bytes are about 292,221,163 bytes for the model, Transformers.js bundle, tested WASM asyncify runtime, and worker combined. The UI shows the exact model payload size, 264,713,119 bytes, and progress against that size. Runtime files are separately served and cached from this AppView. The Transformers.js build is [`@huggingface/transformers` 4.3.2](https://www.npmjs.com/package/@huggingface/transformers/v/4.3.2), Apache-2.0. The vendored ONNX Runtime Web build is `1.31.0-dev.20260914-8d85527a0`, MIT; both license files and runtime provenance are under [`static/vendor/`](static/vendor/README.md). Transformers.js documents [local model/WASM hosting](https://huggingface.co/docs/transformers.js/en/custom_usage) and [WebGPU use](https://huggingface.co/docs/transformers.js/guides/webgpu).

The browser first checks for a WebGPU adapter and tries that backend when available. If no adapter is present it uses WASM with `numThreads = 1` and `proxy = false`; if WebGPU model loading or inference fails, it retries with WASM. The phone’s actual WebGPU availability and caption time still need to be measured on the phone.

## Privacy and CSP

The worker sets `allowRemoteModels = false`, points `localModelPath` at `/static/models/`, and points ONNX Runtime at `/static/vendor/`. The model route is same-origin, streams files, reports content length and type, and applies immutable cache headers. The fetch script pins the Hugging Face revision and checks every downloaded file against SHA-256 before installation.

The CSP remains same-origin for scripts and connections. `script-src` adds only `'wasm-unsafe-eval'`, which ONNX Runtime needs to compile its local WASM binaries. `connect-src 'self'` is unchanged; no COOP/COEP headers were added.

## Setup and phone measurement

1. Fetch and verify the model once: `scripts/fetch-alt-model.sh ./data/alt-models`.
2. Set the parent directory in `appview.toml`: `alt_model_dir = "./data/alt-models"` (or set `APPVIEW_ALT_MODEL_DIR=./data/alt-models`). The files must be under `./data/alt-models/alt-caption-v1/`.
3. Sign in on the iPhone, open `/alt-test`, choose a photo, and tap **Run caption test**. The page reports backend, model download bytes, model load time, caption time, and caption. Run once for first-use download/latency; run again to observe the cached path. No physical iPhone run was performed in this lane.

## Red/green evidence

Red commits: `faaa417` specified the missing route, CSP token, and browser helper; `5d6863d` specified optional model-directory config; `a89c0c0` specified model serving and traversal rejection; `aef8058` specified that posting completes while captioning is pending.

Observed red runs included `/alt-test` returning 404 instead of redirecting to login, the CSP missing `'wasm-unsafe-eval'`, the config assertion failing because `alt_model_dir` was absent, and Deno failures for the missing suggestion/posting helpers. I also temporarily removed the traversal guard: the path-traversal test then received HTTP 200 for the parent file and failed as intended.

Final checks:

- `cargo test --offline -- --test-threads=1`: 237 passed, 2 ignored.
- `deno test --allow-read tests/app_js_test.js`: 7 passed.
- `cargo clippy --offline -- -D warnings`: clean.
- `cargo fmt --check`: clean.
