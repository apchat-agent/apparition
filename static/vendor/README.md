# On-device caption runtime

- `transformers-4.3.2.min.js` is the Apache-2.0 browser build from [`@huggingface/transformers` 4.3.2](https://github.com/huggingface/transformers.js/releases/tag/4.3.2). Its license is in `LICENSE-transformers-js`.
- The ONNX Runtime Web files are from `onnxruntime-web` `1.31.0-dev.20260914-8d85527a0`, MIT-licensed; its license is in `LICENSE-onnxruntime-web`.
- The `asyncify` WASM pair is used by the tested single-thread WASM fallback. The `jsep` pair supplies the WebGPU backend. The regular WASM pair is also vendored for the runtime's fallback selection.

The app embeds and serves these files from this directory. The caption worker sets `wasmPaths` to `/static/vendor/`, disables remote model loading, and uses one WASM thread. The browser makes no request to a package CDN.
