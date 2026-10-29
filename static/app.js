// Like toggles in place instead of reloading the page. Without this script the form posts as before.
document.addEventListener("submit", async (event) => {
  const profileForm = event.target;
  if (profileForm instanceof HTMLFormElement && profileForm.getAttribute("action") === "/profile/action") {
    event.preventDefault();
    const prompt = profileForm.dataset.confirm;
    if (prompt && !window.confirm(prompt)) return;
    const button = profileForm.querySelector("button[type=submit]");
    if (button) button.disabled = true;
    try {
      const response = await fetch("/profile/action", {
        method: "POST",
        body: new URLSearchParams(new FormData(profileForm)),
        headers: { "x-appview-fetch": "1" },
        credentials: "same-origin",
      });
      if (!response.ok) throw new Error(String(response.status));
      const result = await response.json();
      const current = document.querySelector(".profile-actions");
      if (!current || typeof result.actions_html !== "string") throw new Error("profile actions are unavailable");
      const replacement = document.createElement("template");
      replacement.innerHTML = result.actions_html;
      current.replaceWith(replacement.content);
    } catch {
      profileForm.submit();
    }
    return;
  }

  const form = event.target;
  if (!(form instanceof HTMLFormElement) || form.getAttribute("action") !== "/like") return;
  const button = form.querySelector("button");
  if (!button) return;
  event.preventDefault();
  if (button.disabled) return;
  button.disabled = true;
  try {
    const response = await fetch("/like", {
      method: "POST",
      body: new URLSearchParams(new FormData(form)),
      headers: { "x-appview-fetch": "1" },
      credentials: "same-origin",
    });
    if (!response.ok) throw new Error(String(response.status));
    const state = await response.json();
    button.classList.toggle("is-liked", state.liked);
    button.setAttribute("aria-pressed", state.liked ? "true" : "false");
    const label = button.getAttribute("aria-label") || "";
    button.setAttribute("aria-label", label.replace(/^(Unlike|Like)/, state.liked ? "Unlike" : "Like"));
    for (const node of button.childNodes) {
      if (node.nodeType === Node.TEXT_NODE && node.textContent.trim()) {
        node.textContent = state.liked ? "Liked" : "Like";
      }
    }
    // Zero counts are not rendered, so the count element may need creating or removing.
    let count = button.querySelector(".action-count");
    if (state.likeCount > 0) {
      if (!count) {
        count = document.createElement("span");
        count.className = "action-count";
        button.append(count);
      }
      count.textContent = String(state.likeCount);
    } else if (count) {
      count.remove();
    }
    let intent = form.querySelector('input[name="intent"]');
    if (state.liked && !intent) {
      intent = document.createElement("input");
      intent.type = "hidden";
      intent.name = "intent";
      intent.value = "delete";
      form.append(intent);
    } else if (!state.liked && intent) {
      intent.remove();
    }
  } catch {
    form.submit();
  } finally {
    button.disabled = false;
  }
});

async function resizeImageCanvasSource(source, maxBytes = 950_000) {
  const sourceWidth = source.naturalWidth || source.videoWidth || source.width;
  const sourceHeight = source.naturalHeight || source.videoHeight || source.height;
  if (!sourceWidth || !sourceHeight) throw new Error("The selected image could not be decoded.");

  let scale = Math.min(1, 2000 / Math.max(sourceWidth, sourceHeight));
  const canvas = document.createElement("canvas");
  const context = canvas.getContext("2d");
  if (!context) throw new Error("This browser cannot prepare images for upload.");

  // Drawing decoded pixels into a fresh canvas and encoding a new JPEG strips EXIF, including GPS location.
  for (let sizeAttempt = 0; sizeAttempt < 14; sizeAttempt++) {
    const width = Math.max(1, Math.round(sourceWidth * scale));
    const height = Math.max(1, Math.round(sourceHeight * scale));
    canvas.width = width;
    canvas.height = height;
    context.drawImage(source, 0, 0, width, height);
    for (const quality of [0.92, 0.84, 0.76, 0.68, 0.6, 0.52, 0.44, 0.36, 0.28, 0.2, 0.12]) {
      const blob = await new Promise((resolve, reject) => {
        canvas.toBlob((value) => {
          if (value) resolve(value);
          else reject(new Error("The browser could not encode this image."));
        }, "image/jpeg", quality);
      });
      if (blob.size <= maxBytes) return { blob, width, height };
    }
    scale *= 0.85;
  }
  throw new Error("This image could not be reduced below the 950 KB upload limit.");
}

async function loadComposerImage(file) {
  const objectUrl = URL.createObjectURL(file);
  try {
    return await new Promise((resolve, reject) => {
      const image = new Image();
      image.onload = () => resolve(image);
      image.onerror = () => reject(new Error(`Could not open ${file.name || "this image"}.`));
      image.src = objectUrl;
    });
  } finally {
    URL.revokeObjectURL(objectUrl);
  }
}

const ALT_MODEL_DOWNLOAD_BYTES = 264_713_119;
let altCaptionWorker;
let altCaptionRequestId = 0;
const altCaptionRequests = new Map();

function formatAltBytes(bytes) {
  return new Intl.NumberFormat().format(bytes);
}

function getAltCaptionWorker() {
  if (altCaptionWorker) return altCaptionWorker;
  altCaptionWorker = new Worker("/static/alt-model-worker.js", { type: "module" });
  altCaptionWorker.addEventListener("message", (event) => {
    const message = event.data;
    const request = altCaptionRequests.get(message.requestId);
    if (!request) return;
    if (message.type === "progress") {
      request.onProgress?.(message);
      return;
    }
    altCaptionRequests.delete(message.requestId);
    if (message.type === "error") request.reject(new Error(message.message || "The caption model failed to load."));
    else request.resolve(message.result);
  });
  altCaptionWorker.addEventListener("error", () => {
    for (const request of altCaptionRequests.values()) request.reject(new Error("The on-device caption model stopped."));
    altCaptionRequests.clear();
    altCaptionWorker?.terminate();
    altCaptionWorker = undefined;
  });
  return altCaptionWorker;
}

function altCaptionRequest(type, payload = {}, onProgress) {
  const worker = getAltCaptionWorker();
  const requestId = ++altCaptionRequestId;
  return new Promise((resolve, reject) => {
    altCaptionRequests.set(requestId, { resolve, reject, onProgress });
    worker.postMessage({ ...payload, requestId, type });
  });
}

async function loadAltCaptionModel({ onProgress } = {}) {
  const loaded = await altCaptionRequest("load", {}, onProgress);
  return {
    ...loaded,
    caption: async (file, progress) => altCaptionRequest("caption", { file }, progress || onProgress),
  };
}

function attachAltSuggestion(button, item, altInput, status, progress, loadModel = loadAltCaptionModel) {
  let previousSuggestion = item.suggestedAlt || "";
  item.suggestedAlt = previousSuggestion;
  altInput.addEventListener?.("input", () => {
    item.alt = altInput.value;
  });
  button.addEventListener("click", async () => {
    if (button.disabled) return;
    button.disabled = true;
    status.textContent = `Preparing the on-device model (${formatAltBytes(ALT_MODEL_DOWNLOAD_BYTES)} bytes).`;
    progress.max = ALT_MODEL_DOWNLOAD_BYTES;
    progress.value = 0;
    progress.hidden = false;
    const onProgress = (update) => {
      if (Number.isFinite(update.loaded)) progress.value = Math.min(progress.max, update.loaded);
      if (update.message) status.textContent = update.message;
      else if (update.loaded > 0) status.textContent = `Loading model: ${formatAltBytes(progress.value)} of ${formatAltBytes(progress.max)} bytes.`;
    };
    try {
      const model = await loadModel({ onProgress });
      const result = await model.caption(item.file, onProgress);
      const suggestion = (typeof result === "string" ? result : result.caption || "").trim();
      if (!suggestion) throw new Error("No caption was returned.");
      if (!altInput.value.trim() || altInput.value === previousSuggestion) {
        altInput.value = suggestion;
        item.alt = suggestion;
        previousSuggestion = suggestion;
        item.suggestedAlt = suggestion;
      } else {
        item.alt = altInput.value;
      }
      status.textContent = "Suggestion ready. Review and edit it before posting.";
    } catch (reason) {
      const message = reason instanceof Error ? reason.message : "Could not suggest alt text.";
      status.textContent = message.slice(0, 180);
    } finally {
      progress.hidden = true;
      button.disabled = false;
    }
  });
}

async function handleImageComposerSubmit(form, selected, selectionInvalid, event, helpers = {}) {
  const input = form.querySelector("[data-image-input]");
  if (!selected.length && !input?.files?.length) return;
  event.preventDefault();
  const button = form.querySelector('button[type="submit"]');
  const error = form.querySelector("[data-composer-error]");
  if (button?.disabled) return;
  if (button) button.disabled = true;
  error.hidden = true;
  error.textContent = "";
  try {
    if (selectionInvalid) throw new Error("Choose no more than 4 images.");
    const images = [];
    for (const item of selected) {
      const source = await (helpers.loadImage || loadComposerImage)(item.file);
      const resized = await (helpers.resizeImage || resizeImageCanvasSource)(source);
      const filename = (item.file.name || "picture").replace(/\.[^.]*$/, "") + ".jpg";
      images.push({ ...resized, filename, alt: item.alt });
    }
    const data = new FormData(form);
    data.delete("images");
    data.delete("alts");
    for (const image of images) {
      data.append("images", new File([image.blob], image.filename, { type: "image/jpeg" }));
      data.append("alts", image.alt);
      data.append("widths", String(image.width));
      data.append("heights", String(image.height));
    }
    const response = await (helpers.fetch || fetch)(form.action, {
      method: "POST",
      body: data,
      credentials: "same-origin",
    });
    if (!response.ok) {
      let message = "The post could not be created.";
      try {
        message = (await response.json()).message || message;
      } catch {
        // The server can return a plain response for extractor errors.
      }
      throw new Error(message);
    }
    (helpers.navigate || ((url) => window.location.assign(url)))(response.url || "/app");
  } catch (reason) {
    error.textContent = reason instanceof Error ? reason.message : "The post could not be created.";
    error.hidden = false;
  } finally {
    if (button) button.disabled = false;
  }
}

const MAX_VIDEO_BYTES = 300_000_000;
const MAX_VIDEO_DURATION_SECONDS = 600;
const MAX_VIDEO_ALT_CHARS = 2000;

function videoMimeType(file) {
  if (["video/mp4", "video/webm", "video/quicktime"].includes(file.type)) return file.type;
  const extension = (file.name || "").split(".").pop()?.toLowerCase();
  return ({ mp4: "video/mp4", webm: "video/webm", mov: "video/quicktime" })[extension] || "";
}

function formatVideoDuration(seconds) {
  if (!Number.isFinite(seconds) || seconds < 0) return "Unknown duration";
  const whole = Math.floor(seconds);
  const minutes = Math.floor(whole / 60);
  const remainder = String(whole % 60).padStart(2, "0");
  return `${minutes}:${remainder}`;
}

function probeVideoMetadata(videoUrl, createElement = (tag) => document.createElement(tag)) {
  return new Promise((resolve, reject) => {
    const video = createElement("video");
    video.preload = "metadata";
    video.onloadedmetadata = () => resolve({
      duration: video.duration,
      width: video.videoWidth,
      height: video.videoHeight,
    });
    video.onerror = () => reject(new Error("The selected video could not be opened."));
    video.src = videoUrl;
  });
}

async function handleVideoComposerSubmit(form, selected, event, helpers = {}) {
  if (!selected) return;
  event.preventDefault();
  const button = form.querySelector('button[type="submit"]');
  const error = form.querySelector("[data-composer-error]");
  const status = form.querySelector("[data-video-upload-status]");
  const progress = form.querySelector("[data-video-progress]");
  if (button?.disabled) return;
  if (button) button.disabled = true;
  if (error) {
    error.hidden = true;
    error.textContent = "";
  }
  try {
    if (!selected.valid) throw new Error(selected.error || "Wait until video details are ready.");
    if (selected.file.size > MAX_VIDEO_BYTES) throw new Error("Videos can be up to 300 MB.");
    if (selected.metadata.duration > MAX_VIDEO_DURATION_SECONDS) throw new Error("Videos can be up to 10 minutes long.");
    const alt = form.querySelector("[data-video-alt]")?.value || "";
    if ([...alt].length > MAX_VIDEO_ALT_CHARS) throw new Error("Video alt text must be no more than 2,000 characters.");
    const Data = helpers.FormData || FormData;
    const data = new Data(form);
    const FileType = helpers.File || File;
    data.set("video", new FileType([selected.file], selected.file.name || "video", { type: selected.mimeType }));
    data.set("video_duration", String(selected.metadata.duration));
    data.set("video_width", String(selected.metadata.width));
    data.set("video_height", String(selected.metadata.height));
    if (progress) {
      progress.max = 100;
      progress.value = 0;
      progress.hidden = false;
    }
    if (status) status.textContent = "Starting video upload…";
    const Xhr = helpers.XMLHttpRequest || XMLHttpRequest;
    await new Promise((resolve, reject) => {
      const xhr = new Xhr();
      xhr.open("POST", "/video");
      xhr.withCredentials = true;
      xhr.upload.onprogress = (uploadEvent) => {
        if (uploadEvent.lengthComputable && progress) {
          progress.value = Math.min(100, Math.round((uploadEvent.loaded / uploadEvent.total) * 100));
        }
        if (status) {
          status.textContent = uploadEvent.lengthComputable && uploadEvent.loaded >= uploadEvent.total
            ? "Video received. Bluesky is processing it…"
            : uploadEvent.lengthComputable
              ? `Uploading video: ${Math.round((uploadEvent.loaded / uploadEvent.total) * 100)}%`
              : "Uploading video…";
        }
      };
      xhr.onerror = () => reject(new Error("The video upload connection failed."));
      xhr.onload = () => {
        if (xhr.status >= 200 && xhr.status < 400) {
          (helpers.navigate || ((url) => window.location.assign(url)))(xhr.responseURL || "/app");
          resolve();
          return;
        }
        let message = "The video could not be posted.";
        try {
          message = JSON.parse(xhr.responseText).message || message;
        } catch {
          // Keep the concise default for non-JSON server errors.
        }
        reject(new Error(message));
      };
      xhr.send(data);
    });
  } catch (reason) {
    const message = reason instanceof Error ? reason.message : "The video could not be posted.";
    if (error) {
      error.textContent = message;
      error.hidden = false;
    } else if (status) {
      status.textContent = message;
    }
  } finally {
    if (progress) progress.hidden = true;
    if (button) button.disabled = false;
  }
}

function attachVideoComposer(form, helpers = {}) {
  const input = form.querySelector("[data-video-input]");
  const preview = form.querySelector("[data-video-preview]");
  const summary = form.querySelector("[data-video-summary]");
  const removeButton = form.querySelector("[data-video-remove]");
  const imageInput = form.querySelector("[data-image-input]");
  if (!input || !preview || !summary) return null;
  let selected = null;
  let previewUrl = "";
  const makeUrl = helpers.createObjectURL || ((file) => URL.createObjectURL(file));
  const revokeUrl = helpers.revokeObjectURL || ((url) => URL.revokeObjectURL(url));
  const reset = (clearInput = true) => {
    if (previewUrl) revokeUrl(previewUrl);
    previewUrl = "";
    selected = null;
    if (clearInput) input.value = "";
    preview.removeAttribute("src");
    preview.hidden = true;
    summary.textContent = "No video selected.";
    if (removeButton) removeButton.hidden = true;
    if (imageInput) imageInput.disabled = false;
  };
  input.addEventListener("change", async () => {
    const file = input.files?.[0];
    reset(false);
    if (!file) return;
    const mimeType = videoMimeType(file);
    if (!mimeType) {
      summary.textContent = "Choose an MP4, WebM, or MOV video.";
      selected = { file, valid: false, error: summary.textContent };
      if (imageInput) imageInput.disabled = true;
      return;
    }
    if (file.size > MAX_VIDEO_BYTES) {
      summary.textContent = "Videos can be up to 300 MB. Choose a smaller file.";
      selected = { file, valid: false, error: summary.textContent };
      if (imageInput) imageInput.disabled = true;
      return;
    }
    if (imageInput?.files?.length) {
      summary.textContent = "Remove the selected images before choosing a video.";
      input.value = "";
      return;
    }
    if (imageInput) imageInput.disabled = true;
    previewUrl = makeUrl(file);
    preview.src = previewUrl;
    preview.hidden = false;
    if (removeButton) removeButton.hidden = false;
    selected = { file, mimeType, valid: false, metadata: null };
    summary.textContent = `Video size: ${new Intl.NumberFormat().format(file.size)} bytes. Reading duration…`;
    try {
      const metadata = await (helpers.probe || probeVideoMetadata)(previewUrl, helpers.createElement);
      if (selected?.file !== file) return;
      if (!Number.isFinite(metadata.duration) || metadata.duration <= 0) {
        throw new Error("The video duration could not be read.");
      }
      selected.metadata = metadata;
      selected.valid = metadata.duration <= MAX_VIDEO_DURATION_SECONDS && metadata.width > 0 && metadata.height > 0;
      selected.error = metadata.duration > MAX_VIDEO_DURATION_SECONDS
        ? "Videos can be up to 10 minutes long."
        : "The video dimensions could not be read.";
      summary.textContent = `${new Intl.NumberFormat().format(file.size)} bytes · ${formatVideoDuration(metadata.duration)} · ${metadata.width}×${metadata.height}${selected.valid ? "" : ` · ${selected.error}`}`;
    } catch (reason) {
      if (selected?.file !== file) return;
      selected.error = reason instanceof Error ? reason.message : "Video details could not be read.";
      summary.textContent = `${new Intl.NumberFormat().format(file.size)} bytes · ${selected.error}`;
    }
  });
  removeButton?.addEventListener("click", reset);
  form.addEventListener("submit", (event) => {
    if (selected) void handleVideoComposerSubmit(form, selected, event, helpers);
  });
  return {
    setImagesSelected(selectedImages) {
      input.disabled = selectedImages;
    },
  };
}

function previewImageObjectUrl(dataUrl) {
  const match = /^data:(image\/[^;,]+);base64,([A-Za-z0-9+/]+=*)$/.exec(dataUrl || "");
  if (!match) return "";
  const binary = atob(match[2]);
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index++) bytes[index] = binary.charCodeAt(index);
  return URL.createObjectURL(new Blob([bytes], { type: match[1] }));
}

function attachLinkPreview(form, helpers = {}) {
  const input = form.querySelector("[data-link-input]");
  const button = form.querySelector("[data-link-preview]");
  const status = form.querySelector("[data-link-status]");
  const preview = form.querySelector("[data-link-preview-card]");
  const imageNote = form.querySelector("[data-link-image-note]");
  if (!input || !button || !status || !preview) {
    return { updateImagePrecedence() {} };
  }

  let thumbnailObjectUrl = "";
  let selectedImageCount = 0;
  const clearPreview = () => {
    if (thumbnailObjectUrl) URL.revokeObjectURL(thumbnailObjectUrl);
    thumbnailObjectUrl = "";
    preview.replaceChildren();
    preview.hidden = true;
  };
  const updateImagePrecedence = (imageCount) => {
    selectedImageCount = imageCount;
    if (imageNote) imageNote.hidden = !(imageCount > 0 && input.value.trim());
  };
  const removeLink = () => {
    input.value = "";
    clearPreview();
    status.textContent = "Webpage removed.";
    updateImagePrecedence(0);
  };

  input.addEventListener("input", () => {
    clearPreview();
    status.textContent = "";
    updateImagePrecedence(selectedImageCount);
  });
  button.addEventListener("click", async () => {
    const url = input.value.trim();
    if (!url || button.disabled) return;
    button.disabled = true;
    status.textContent = "Loading webpage preview…";
    clearPreview();
    try {
      const response = await (helpers.fetch || fetch)("/link-preview", {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8" },
        body: new URLSearchParams({ url }),
        credentials: "same-origin",
      });
      let result;
      try {
        result = await response.json();
      } catch {
        result = {};
      }
      if (!response.ok) throw new Error(result.message || "The webpage preview could not be loaded.");

      const card = document.createElement("article");
      card.className = "external-card composer-link-card";
      if (result.thumbnail) {
        thumbnailObjectUrl = previewImageObjectUrl(result.thumbnail);
        if (thumbnailObjectUrl) {
          const image = document.createElement("img");
          image.src = thumbnailObjectUrl;
          image.alt = "";
          image.loading = "lazy";
          card.append(image);
        }
      }
      const title = document.createElement("p");
      title.textContent = result.title || result.uri || url;
      card.append(title);
      if (result.description) {
        const description = document.createElement("p");
        description.textContent = result.description;
        card.append(description);
      }
      const link = document.createElement("a");
      link.href = result.uri || url;
      link.rel = "nofollow noopener noreferrer";
      link.textContent = result.uri || url;
      card.append(link);
      const remove = document.createElement("button");
      remove.type = "button";
      remove.textContent = "Remove webpage";
      remove.addEventListener("click", removeLink);
      card.append(remove);
      preview.append(card);
      preview.hidden = false;
      status.textContent = "Webpage preview ready.";
    } catch (reason) {
      status.textContent = reason instanceof Error
        ? reason.message.slice(0, 180)
        : "The webpage preview could not be loaded.";
    } finally {
      button.disabled = false;
      updateImagePrecedence(0);
    }
  });
  return { updateImagePrecedence };
}

(document.querySelectorAll?.("form[data-image-composer]") || []).forEach((form) => {
  const linkPreview = attachLinkPreview(form);
  const videoComposer = attachVideoComposer(form);
  const input = form.querySelector("[data-image-input]");
  const previews = form.querySelector("[data-image-previews]");
  const status = form.querySelector("[data-image-status]");
  if (!input || !previews || !status) return;

  const selected = [];
  let selectionInvalid = false;
  const drawPreviews = () => {
    previews.replaceChildren();
    selected.forEach((item, index) => {
      const preview = document.createElement("div");
      preview.className = "image-preview";
      const image = document.createElement("img");
      image.src = item.previewUrl;
      image.alt = "";
      const details = document.createElement("div");
      const altLabel = document.createElement("label");
      altLabel.textContent = `Alt text for image ${index + 1} (optional)`;
      const altInput = document.createElement("textarea");
      altInput.name = "alts";
      altInput.rows = 2;
      altInput.maxLength = 2000;
      altInput.value = item.alt;
      altInput.addEventListener("input", () => {
        item.alt = altInput.value;
      });
      altLabel.append(altInput);
      const suggestion = document.createElement("button");
      suggestion.type = "button";
      suggestion.className = "alt-suggestion-button";
      suggestion.textContent = "Suggest alt text";
      const suggestionStatus = document.createElement("p");
      suggestionStatus.className = "alt-suggestion-status";
      suggestionStatus.setAttribute("role", "status");
      const suggestionProgress = document.createElement("progress");
      suggestionProgress.className = "alt-suggestion-progress";
      suggestionProgress.hidden = true;
      suggestionProgress.max = ALT_MODEL_DOWNLOAD_BYTES;
      suggestionProgress.value = 0;
      attachAltSuggestion(suggestion, item, altInput, suggestionStatus, suggestionProgress);
      const remove = document.createElement("button");
      remove.type = "button";
      remove.textContent = "Remove";
      remove.setAttribute("aria-label", `Remove image ${index + 1}`);
      remove.addEventListener("click", () => {
        URL.revokeObjectURL(item.previewUrl);
        selected.splice(index, 1);
        status.textContent = "";
        videoComposer?.setImagesSelected(selected.length > 0);
        linkPreview.updateImagePrecedence(selected.length);
        drawPreviews();
      });
      details.append(altLabel, suggestion, suggestionProgress, suggestionStatus, remove);
      preview.append(image, details);
      previews.append(preview);
    });
  };

  input.addEventListener("change", () => {
    selected.forEach((item) => URL.revokeObjectURL(item.previewUrl));
    selected.length = 0;
    linkPreview.updateImagePrecedence(0);
    const files = [...(input.files || [])];
    videoComposer?.setImagesSelected(files.length > 0);
    if (files.length > 4) {
      selectionInvalid = true;
      status.textContent = "Choose no more than 4 images.";
      drawPreviews();
      return;
    }
    selectionInvalid = false;
    status.textContent = "";
    for (const file of files) {
      selected.push({ file, alt: "", previewUrl: URL.createObjectURL(file) });
    }
    videoComposer?.setImagesSelected(selected.length > 0);
    linkPreview.updateImagePrecedence(selected.length);
    drawPreviews();
  });

  form.addEventListener("submit", (event) => {
    void handleImageComposerSubmit(form, selected, selectionInvalid, event);
  });
});

const altTestForm = document.querySelector?.("form[data-alt-test]");
if (altTestForm) {
  const imageInput = altTestForm.querySelector("[data-alt-test-image]");
  const runButton = altTestForm.querySelector("[data-alt-test-run]");
  const progress = document.querySelector("[data-alt-test-progress]");
  const status = document.querySelector("[data-alt-test-status]");
  const metrics = {
    backend: document.querySelector("[data-alt-test-backend]"),
    bytes: document.querySelector("[data-alt-test-bytes]"),
    loadMs: document.querySelector("[data-alt-test-load-ms]"),
    captionMs: document.querySelector("[data-alt-test-caption-ms]"),
    caption: document.querySelector("[data-alt-test-caption]"),
  };
  if (imageInput && runButton && progress && status) {
    altTestForm.addEventListener("submit", async (event) => {
      event.preventDefault();
      const file = imageInput.files?.[0];
      if (!file) {
        status.textContent = "Choose a photo first.";
        return;
      }
      runButton.disabled = true;
      progress.max = ALT_MODEL_DOWNLOAD_BYTES;
      progress.value = 0;
      progress.hidden = false;
      status.textContent = `Preparing the on-device model (${formatAltBytes(ALT_MODEL_DOWNLOAD_BYTES)} bytes).`;
      const onProgress = (update) => {
        if (Number.isFinite(update.loaded)) progress.value = Math.min(progress.max, update.loaded);
        if (update.message) status.textContent = update.message;
      };
      try {
        const model = await loadAltCaptionModel({ onProgress });
        const result = await model.caption(file, onProgress);
        metrics.backend.textContent = result.backend || model.backend || "—";
        metrics.bytes.textContent = formatAltBytes(result.downloadBytes || model.downloadBytes || 0);
        metrics.loadMs.textContent = String(result.modelLoadMs ?? model.modelLoadMs ?? 0);
        metrics.captionMs.textContent = String(result.captionMs ?? 0);
        metrics.caption.textContent = result.caption || "No caption was returned.";
        status.textContent = "Caption complete. The selected photo stayed in this browser.";
      } catch (reason) {
        const message = reason instanceof Error ? reason.message : "The on-device caption could not be created.";
        status.textContent = message.slice(0, 180);
      } finally {
        progress.hidden = true;
        runButton.disabled = false;
      }
    });
  }
}

function installInfiniteScroll({
  pageDocument = document,
  pageWindow = window,
  fetchPage = (...args) => fetch(...args),
  parsePage = (html, selector) => {
    const parsed = new DOMParser().parseFromString(html, "text/html");
    return {
      list: parsed.querySelector(selector),
      link: parsed.querySelector('.pagination-link[rel="next"]'),
    };
  },
  Observer = globalThis.IntersectionObserver,
} = {}) {
  const maximumCards = 500;
  const olderLink = pageDocument.querySelector('.pagination-link[rel="next"]');
  const list = pageDocument.querySelector(".post-list, .notification-list");
  if (!olderLink || !list) return;

  const isNotificationList = list.classList?.contains("notification-list") ||
    list.className.split(/\s+/).includes("notification-list");
  const listSelector = isNotificationList ? ".notification-list" : ".post-list";
  const layout = pageDocument.querySelector(".app-layout");
  const isNarrow = pageWindow.matchMedia?.("(max-width: 799px)").matches || false;
  const scrollRoot = pageDocument.querySelector(".mobile-nav") && isNarrow ? layout : null;
  const scrollTarget = scrollRoot || pageWindow;
  const status = pageDocument.createElement("p");
  status.className = "infinite-scroll-status";
  status.hidden = true;
  status.setAttribute("role", "status");
  status.setAttribute("aria-live", "polite");
  const retry = pageDocument.createElement("button");
  retry.className = "infinite-scroll-retry";
  retry.type = "button";
  retry.textContent = "Retry";
  retry.hidden = true;
  olderLink.after(status, retry);

  let sentinel;
  let inFlight = false;
  let stopped = false;
  let retryRequired = false;

  const pageCards = () => [...list.children].filter((item) => !item.className?.split(/\s+/).includes("empty-state"));
  const positionSentinel = (cards = pageCards()) => {
    if (!cards.length) {
      sentinel = undefined;
      return;
    }
    const nextSentinel = cards[Math.floor((cards.length - 1) / 2)];
    if (sentinel && sentinel !== nextSentinel) observer?.unobserve(sentinel);
    sentinel = nextSentinel;
    sentinel.setAttribute("data-infinite-scroll-sentinel", "");
    observer?.observe(sentinel);
  };

  const observer = Observer ? new Observer(() => checkPosition(), { root: scrollRoot, threshold: 0 }) : null;

  const rootTop = () => scrollRoot?.getBoundingClientRect().top ?? 0;
  const updateStatus = (message) => {
    status.textContent = message;
    status.hidden = !message;
  };

  const finish = (message) => {
    stopped = true;
    observer?.disconnect();
    updateStatus(message);
    olderLink.remove();
  };

  const stopAtMemoryLimit = () => {
    const retainedCards = pageCards().length;
    stopped = true;
    observer?.disconnect();
    retry.hidden = true;
    updateStatus(`Showing ${retainedCards} items. Use Older to continue.`);
  };

  const postNodesIn = (root) => {
    const found = [];
    const visit = (node) => {
      if (node.getAttribute?.("data-post-uri")) found.push(node);
      for (const child of node.children || []) visit(child);
    };
    visit(root);
    return found;
  };

  const hasClass = (node, name) => node.className?.split(/\s+/).includes(name);

  const nodeWithClassIn = (root, name) => {
    if (hasClass(root, name)) return root;
    for (const child of root.children || []) {
      const found = nodeWithClassIn(child, name);
      if (found) return found;
    }
    return null;
  };

  const threadGroupFor = (node) => {
    let current = node;
    while (current && current !== list) {
      if (hasClass(current, "thread-group")) return current;
      current = current.parentElement;
    }
    return null;
  };

  const appendCards = (nextList) => {
    const existingPostNodes = new Map();
    const existingParentNodes = new Map();
    for (const node of postNodesIn(list)) {
      const uri = node.getAttribute("data-post-uri");
      const destination = node.getAttribute("data-post-kind") === "parent" ? existingParentNodes : existingPostNodes;
      if (!destination.has(uri)) destination.set(uri, node);
    }
    for (const empty of [...list.children].filter((item) => item.className?.split(/\s+/).includes("empty-state"))) {
      empty.remove();
    }
    const appended = [];
    const newCards = [...nextList.children].filter((item) => !item.className?.split(/\s+/).includes("empty-state"));
    for (const card of newCards) {
      const nodes = postNodesIn(card);
      const posts = nodes.filter((node) => node.getAttribute("data-post-kind") !== "parent");
      const duplicates = posts.filter((node) => existingPostNodes.has(node.getAttribute("data-post-uri")));
      const freshPosts = posts.filter((node) => !existingPostNodes.has(node.getAttribute("data-post-uri")));
      if (duplicates.length) {
        const existingGroup = duplicates
          .map((node) => threadGroupFor(existingPostNodes.get(node.getAttribute("data-post-uri"))))
          .find(Boolean);
        if (existingGroup && hasClass(card, "thread-group") && freshPosts.length) {
          for (const duplicate of duplicates) duplicate.remove();
          const threadMore = nodeWithClassIn(existingGroup, "thread-more");
          for (const post of freshPosts) {
            const connector = document.createElement("span");
            connector.className = "reply-connector thread-group-connector";
            connector.setAttribute("aria-hidden", "true");
            post.remove();
            if (threadMore && typeof existingGroup.insertBefore === "function") {
              existingGroup.insertBefore(connector, threadMore);
              existingGroup.insertBefore(post, threadMore);
            } else {
              existingGroup.append(connector, post);
            }
            initializeVideoPlayers(post);
            existingPostNodes.set(post.getAttribute("data-post-uri"), post);
          }
          if (threadMore) {
            const moreMatch = threadMore.textContent.trim().match(/^(\d+) more (replies|in this thread)$/);
            if (moreMatch) {
              const remaining = Math.max(0, Number(moreMatch[1]) - freshPosts.length);
              if (remaining === 0) {
                threadMore.remove();
              } else {
                const label = moreMatch[2] === "replies"
                  ? remaining === 1 ? "reply" : "replies"
                  : "in this thread";
                threadMore.textContent = `${remaining} more ${label}`;
              }
            }
          }
          card.remove();
          if (!appended.includes(existingGroup)) appended.push(existingGroup);
          continue;
        }
        for (const duplicate of duplicates) duplicate.remove();
      }
      if (!freshPosts.length) {
        card.remove();
        continue;
      }
      for (const parent of nodes.filter((node) => node.getAttribute("data-post-kind") === "parent")) {
        const uri = parent.getAttribute("data-post-uri");
        if (existingPostNodes.has(uri) || existingParentNodes.has(uri)) {
          parent.remove();
          continue;
        }
        existingParentNodes.set(uri, parent);
      }
      for (const post of freshPosts) {
        const uri = post.getAttribute("data-post-uri");
        existingPostNodes.set(uri, post);
      }
      list.append(card);
      initializeVideoPlayers(card);
      appended.push(card);
    }
    return appended;
  };

  const loadNextPage = async (manualRetry = false) => {
    if (stopped || inFlight || (retryRequired && !manualRetry)) return;
    if (pageCards().length >= maximumCards) {
      stopAtMemoryLimit();
      return;
    }
    const href = olderLink.getAttribute("href") || olderLink.href;
    if (!href) {
      finish("You’re all caught up.");
      return;
    }
    inFlight = true;
    retryRequired = false;
    retry.hidden = true;
    updateStatus("Loading older posts…");
    try {
      const response = await fetchPage(href, {
        credentials: "same-origin",
        headers: { Accept: "text/html" },
      });
      if (!response.ok) throw new Error(`Request failed (${response.status}).`);
      const page = parsePage(await response.text(), listSelector);
      if (!page?.list) throw new Error("The next page could not be read.");
      const incomingCount = [...page.list.children]
        .filter((item) => !item.className?.split(/\s+/).includes("empty-state")).length;
      if (pageCards().length + incomingCount > maximumCards) {
        stopAtMemoryLimit();
        return;
      }
      const appended = appendCards(page.list);
      if (!page.link) {
        finish("You’re all caught up.");
        return;
      }
      const nextHref = page.link.getAttribute("href") || page.link.href;
      if (!nextHref) {
        finish("You’re all caught up.");
        return;
      }
      olderLink.setAttribute("href", nextHref);
      olderLink.href = nextHref;
      olderLink.textContent = page.link.textContent || "Older";
      updateStatus("");
      positionSentinel(appended.length ? appended : pageCards());
    } catch {
      retryRequired = true;
      updateStatus("Couldn’t load more. Try again.");
      retry.hidden = false;
    } finally {
      inFlight = false;
    }
  };

  function checkPosition() {
    if (stopped || inFlight || retryRequired || !sentinel) return;
    if (sentinel.getBoundingClientRect().top <= rootTop()) void loadNextPage();
  }

  retry.addEventListener("click", () => void loadNextPage(true));
  scrollTarget.addEventListener("scroll", checkPosition, { passive: true });
  positionSentinel();
  checkPosition();
}

function initializeVideoPlayers(root = document) {
  const videos = [];
  if (root.matches?.("video[data-video-playlist]")) videos.push(root);
  videos.push(...(root.querySelectorAll?.("video[data-video-playlist]") || []));
  for (const video of videos) {
    if (video.dataset?.videoInitialized === "true") continue;
    const playlist = video.getAttribute("data-video-playlist");
    if (!playlist) continue;
    if (video.canPlayType?.("application/vnd.apple.mpegurl")) {
      video.src = playlist;
      if (video.dataset) {
        video.dataset.videoReady = "true";
        video.dataset.videoInitialized = "true";
      }
      continue;
    }
    const HlsClient = window.Hls || globalThis.Hls;
    if (!HlsClient?.isSupported?.()) continue;
    const player = new HlsClient({ enableWorker: false });
    player.loadSource(playlist);
    player.attachMedia(video);
    if (video.dataset) {
      video.dataset.videoReady = "true";
      video.dataset.videoInitialized = "true";
    }
  }
}

installInfiniteScroll();
initializeVideoPlayers();

// Phones: the app frame is pinned to the screen (app.css), so the document itself should never be scrolled. iOS can
// still leave it scrolled after the keyboard closes; put it back.
const resetDocumentScroll = () => {
  if (document.querySelector(".mobile-nav") && (window.scrollY !== 0 || window.scrollX !== 0)) window.scrollTo(0, 0);
};
document.addEventListener("focusout", () => setTimeout(resetDocumentScroll, 100));
window.visualViewport?.addEventListener("resize", resetDocumentScroll);

// Keep the notification badge live while the page is open. SSE is the normal path;
// the JSON count endpoint provides a low-frequency fallback for proxies that close streams.
(() => {
  const badges = [...document.querySelectorAll("[data-notification-badge]")];
  if (!badges.length) return;

  const updateBadges = (value) => {
    const count = Number(value);
    if (!Number.isSafeInteger(count) || count < 0) return;
    for (const badge of badges) {
      badge.hidden = count === 0;
      badge.textContent = count > 9 ? "9+" : String(count);
      badge.setAttribute("aria-label", `${count} unread notifications`);
    }
  };

  const refreshCount = async () => {
    try {
      const response = await fetch("/notifications/unread-count", {
        credentials: "same-origin",
        headers: { Accept: "application/json" },
      });
      if (!response.ok) return;
      const result = await response.json();
      updateBadges(result.count);
    } catch {
      // The next visibility change or polling tick retries the small count request.
    }
  };

  let pollTimer;
  const startPolling = () => {
    if (pollTimer) return;
    refreshCount();
    pollTimer = window.setInterval(refreshCount, 30_000);
  };

  if ("EventSource" in window) {
    const source = new EventSource("/notifications/stream");
    source.addEventListener("unread", (event) => updateBadges(event.data));
    source.onerror = () => {
      source.close();
      startPolling();
    };
  } else {
    startPolling();
  }

  document.addEventListener("visibilitychange", () => {
    if (!document.hidden) refreshCount();
  });
})();

// A quote can remain unresolved when page hydration reaches its shared budget. Load
// its same-origin fragment after the page is visible, keeping a useful link if it fails.
function fillDeferredQuotes(root = document) {
  const placeholders = [];
  if (root.matches?.("[data-quote-fragment]")) placeholders.push(root);
  placeholders.push(...(root.querySelectorAll?.("[data-quote-fragment]") || []));
  for (const placeholder of placeholders) {
    if (placeholder.dataset?.quoteLoading === "true") continue;
    const fragmentUrl = placeholder.getAttribute("data-quote-fragment");
    if (!fragmentUrl) continue;
    if (placeholder.dataset) placeholder.dataset.quoteLoading = "true";
    fetch(fragmentUrl, {
      credentials: "same-origin",
      headers: { Accept: "text/html" },
    }).then(async (response) => {
      if (!response.ok) throw new Error(`Quote fragment request failed (${response.status}).`);
      const fragmentDocument = new DOMParser().parseFromString(await response.text(), "text/html");
      const card = fragmentDocument.querySelector(".quoted-post-card");
      if (!card) throw new Error("The quote fragment did not contain a post card.");
      fillDeferredQuotes(card);
      placeholder.replaceWith(card);
    }).catch(() => {
      const fallback = document.createElement("a");
      fallback.className = "quoted-post-card quoted-post-status";
      fallback.href = placeholder.getAttribute("data-quote-thread") || "/app";
      fallback.textContent = "Quoted post unavailable · Open thread";
      placeholder.replaceWith(fallback);
    });
  }
}

fillDeferredQuotes();
