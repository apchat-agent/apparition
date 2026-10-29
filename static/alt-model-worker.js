import {
  AutoProcessor,
  RawImage,
  SmolVLMForConditionalGeneration,
  env,
} from "/static/vendor/transformers-4.3.2.min.js";

const MODEL_ID = "alt-caption-v1";
const MODEL_DOWNLOAD_BYTES = 264_713_119;
let processor;
let model;
let backend;
let modelLoadMs = 0;
let loading;
let transferProgress = new Map();
let requestQueue = Promise.resolve();

env.allowRemoteModels = false;
env.allowLocalModels = true;
env.localModelPath = "/static/models/";
env.backends.onnx.wasm.wasmPaths = "/static/vendor/";
env.backends.onnx.wasm.numThreads = 1;
env.backends.onnx.wasm.proxy = false;

function reportProgress(requestId, item) {
  const name = item.file || item.name || "model";
  const loaded = Number(item.loaded ?? (item.total ? item.progress * item.total : 0));
  transferProgress.set(name, Math.max(transferProgress.get(name) || 0, loaded));
  const total = Math.min(
    MODEL_DOWNLOAD_BYTES,
    [...transferProgress.values()].reduce((sum, value) => sum + value, 0),
  );
  self.postMessage({
    type: "progress",
    requestId,
    loaded: total,
    total: MODEL_DOWNLOAD_BYTES,
    message: item.status === "progress"
      ? `Loading model: ${new Intl.NumberFormat().format(total)} of ${new Intl.NumberFormat().format(MODEL_DOWNLOAD_BYTES)} bytes.`
      : "Preparing the on-device caption model.",
  });
}

async function loadModel(requestId) {
  if (model) return { backend, modelLoadMs, downloadBytes: MODEL_DOWNLOAD_BYTES };
  if (!loading) {
    loading = (async () => {
      const started = performance.now();
      const progress_callback = (item) => reportProgress(requestId, item);
      processor = await AutoProcessor.from_pretrained(MODEL_ID, {
        progress_callback,
        local_files_only: true,
      });
      let hasWebGpuAdapter = false;
      try {
        hasWebGpuAdapter = Boolean(await self.navigator?.gpu?.requestAdapter());
      } catch {
        hasWebGpuAdapter = false;
      }
      if (hasWebGpuAdapter) {
        try {
          model = await SmolVLMForConditionalGeneration.from_pretrained(MODEL_ID, {
            device: "webgpu",
            dtype: "int8",
            local_files_only: true,
            progress_callback,
          });
          backend = "webgpu";
        } catch {
          model = undefined;
        }
      }
      if (!model) {
        model = await SmolVLMForConditionalGeneration.from_pretrained(MODEL_ID, {
          device: "wasm",
          dtype: "int8",
          local_files_only: true,
          progress_callback,
        });
        backend = "wasm";
      }
      modelLoadMs = Math.round(performance.now() - started);
      return { backend, modelLoadMs, downloadBytes: MODEL_DOWNLOAD_BYTES };
    })();
  }
  try {
    return await loading;
  } catch (error) {
    loading = undefined;
    processor = undefined;
    model = undefined;
    throw error;
  }
}

async function loadWasmModel(requestId) {
  const started = performance.now();
  await model?.dispose?.();
  model = await SmolVLMForConditionalGeneration.from_pretrained(MODEL_ID, {
    device: "wasm",
    dtype: "int8",
    local_files_only: true,
    progress_callback: (item) => reportProgress(requestId, item),
  });
  backend = "wasm";
  modelLoadMs += Math.round(performance.now() - started);
}

async function caption(file, requestId) {
  await loadModel(requestId);
  const started = performance.now();
  const image = await RawImage.read(file);
  const messages = [{
    role: "user",
    content: [
      { type: "image" },
      { type: "text", text: "Describe this picture in one short sentence." },
    ],
  }];
  const prompt = processor.apply_chat_template(messages, { add_generation_prompt: true });
  const inputs = await processor(prompt, [image]);
  const generate = async () => {
    const generated = await model.generate({ ...inputs, max_new_tokens: 48, do_sample: false });
    const promptLength = inputs.input_ids.dims[1];
    const answer = generated.slice(null, [promptLength, null]);
    const decoded = await processor.batch_decode(answer, { skip_special_tokens: true });
    return String(decoded[0] || "").trim();
  };
  let caption;
  try {
    caption = await generate();
  } catch (error) {
    if (backend !== "webgpu") throw error;
    await loadWasmModel(requestId);
    caption = await generate();
  }
  return {
    caption,
    captionMs: Math.round(performance.now() - started),
    backend,
    modelLoadMs,
    downloadBytes: MODEL_DOWNLOAD_BYTES,
  };
}

async function handle(message) {
  try {
    if (message.type === "load") {
      self.postMessage({ type: "result", requestId: message.requestId, result: await loadModel(message.requestId) });
    } else if (message.type === "caption") {
      self.postMessage({
        type: "result",
        requestId: message.requestId,
        result: await caption(message.file, message.requestId),
      });
    } else {
      throw new Error("Unknown caption worker request.");
    }
  } catch (error) {
    self.postMessage({
      type: "error",
      requestId: message.requestId,
      message: error instanceof Error ? error.message : "The caption model failed.",
    });
  }
}

self.addEventListener("message", (event) => {
  const message = event.data;
  requestQueue = requestQueue.then(() => handle(message), () => handle(message));
});
