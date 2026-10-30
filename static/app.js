document.addEventListener("change", (event) => {
  const select = event.target;
  if (!(select instanceof HTMLSelectElement) || !select.matches("[data-profile-list-select]")) return;
  const option = select.selectedOptions[0];
  const button = select.form?.querySelector(".profile-list-action");
  if (button && option) button.textContent = option.dataset.member === "true" ? "Remove" : "Add";
});

document.addEventListener("click", async (event) => {
  const target = event.target;
  const cancel = target.closest?.(".block-confirm-dialog .secondary-button");
  if (cancel) {
    event.preventDefault();
    const dialog = cancel.closest("dialog");
    dialog?.close();
    dialog?.remove();
    return;
  }

  const listLink = target.closest?.("a[data-focus-list]");
  if (listLink) {
    event.preventDefault();
    listLink.closest("details")?.removeAttribute("open");
    const select = document.querySelector("[data-profile-list-select]");
    select?.scrollIntoView({ block: "center" });
    select?.focus();
    return;
  }

  const link = target.closest?.("a[data-block-confirm]");
  if (!link) return;
  event.preventDefault();
  link.closest("details")?.removeAttribute("open");
  try {
    const response = await fetch(link.href, {
      credentials: "same-origin",
      headers: { Accept: "text/html" },
    });
    if (!response.ok) throw new Error(String(response.status));
    const page = new DOMParser().parseFromString(await response.text(), "text/html");
    const card = page.querySelector(".block-confirm-card");
    if (!card) throw new Error("the block confirmation page is unavailable");
    const dialog = document.createElement("dialog");
    dialog.className = "block-confirm-dialog";
    dialog.append(card.cloneNode(true));
    dialog.addEventListener("close", () => dialog.remove(), { once: true });
    document.body.append(dialog);
    dialog.showModal();
  } catch {
    window.location.assign(link.href);
  }
});

// Like and profile actions update in place when this script is enabled. Their forms still work without it.
document.addEventListener("submit", async (event) => {
  const profileForm = event.target;
  if (profileForm instanceof HTMLFormElement && profileForm.getAttribute("action") === "/profile/action") {
    event.preventDefault();
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
      const dialog = profileForm.closest("dialog");
      if (dialog) {
        dialog.close();
        dialog.remove();
      }
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

async function handleImageComposerSubmit(form, selected, selectionInvalid, event, helpers = {}) {
  const input = form.querySelector("[data-media-input]");
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
    data.delete("media");
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

function selectComposerMedia(current, incoming, videoAccess = true, imageAccess = true) {
  const existing = current || [];
  const files = [...(incoming || [])];
  if (!files.length) {
    return { kind: existing.some((item) => item.kind === "video") ? "video" : existing.length ? "images" : "none", items: existing, message: "" };
  }

  const kinds = files.map((file) => {
    if ((file.type || "").startsWith("image/") || /\.(jpe?g|png|webp|gif)$/i.test(file.name || "")) return "image";
    if ((file.type || "").startsWith("video/") || videoMimeType(file)) return "video";
    return "unsupported";
  });
  const unchanged = (message) => ({ kind: "error", items: existing, message });
  if (kinds.includes("unsupported")) return unchanged("Choose images or one MP4, WebM, or MOV video.");
  if (new Set(kinds).size > 1) return unchanged("Choose either images or one video, not both.");

  const kind = kinds[0];
  if (kind === "image") {
    if (!imageAccess) return unchanged("Log in again to enable pictures.");
    if (existing.some((item) => item.kind === "video")) {
      return unchanged("Remove the selected video before choosing images.");
    }
    if (existing.length + files.length > 4) return unchanged("Choose no more than 4 images.");
    return { kind: "images", items: [...existing, ...files.map((file) => ({ file, kind: "image" }))], message: "" };
  }

  if (!files.every((file) => videoMimeType(file))) return unchanged("Choose an MP4, WebM, or MOV video.");
  if (!videoAccess) return unchanged("sign in again to post videos");
  if (existing.some((item) => item.kind === "image")) {
    return unchanged("Remove the selected images before choosing a video.");
  }
  if (existing.length + files.length > 1) return unchanged("Choose one video per post.");
  return { kind: "video", items: [...existing, ...files.map((file) => ({ file, kind: "video" }))], message: "" };
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
    let metadata;
    let complete = false;
    let fallback;
    const finish = (posterUrl = "") => {
      if (complete || !metadata) return;
      complete = true;
      clearTimeout(fallback);
      resolve({ ...metadata, posterUrl });
    };
    const capturePoster = () => {
      if (complete || !metadata) return;
      let posterUrl = "";
      try {
        const canvas = createElement("canvas");
        const scale = Math.min(1, 640 / metadata.width);
        canvas.width = Math.max(1, Math.round(metadata.width * scale));
        canvas.height = Math.max(1, Math.round(metadata.height * scale));
        const context = canvas.getContext("2d");
        if (context) {
          context.drawImage(video, 0, 0, canvas.width, canvas.height);
          posterUrl = previewImageObjectUrl(canvas.toDataURL("image/jpeg", 0.82));
        }
      } catch {
        // Some codecs cannot be drawn to canvas; the local video preview remains available.
      }
      finish(posterUrl);
    };
    video.onloadedmetadata = () => {
      metadata = {
        duration: video.duration,
        width: video.videoWidth,
        height: video.videoHeight,
      };
      fallback = setTimeout(() => finish(), 1200);
      video.onseeked = capturePoster;
      try {
        video.currentTime = Math.min(0.5, video.duration / 2);
      } catch {
        finish();
      }
    };
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
    data.delete("media");
    data.delete("images");
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

function enhanceCompactComposer(form) {
  const textarea = form.querySelector("[data-compose-input]");
  if (!textarea) return null;

  const counter = form.querySelector("[data-character-count]");
  const mediaButton = form.querySelector("[data-compose-media]");
  const mediaInput = form.querySelector("[data-media-input]");
  const mediaPicker = form.querySelector("[data-media-picker]");
  const linkButton = form.querySelector("[data-link-toggle]");
  const linkPicker = form.querySelector("[data-link-picker]");
  const updateCounter = () => {
    const length = [...textarea.value].length;
    if (counter) {
      counter.textContent = `${length}/300`;
      counter.classList.toggle("is-near-limit", length >= 270);
    }
  };
  const resize = () => {
    const expanded = form.classList.contains("is-expanded");
    if (!expanded && !textarea.value) {
      textarea.rows = 1;
      return;
    }
    const computed = window.getComputedStyle?.(textarea);
    const lineHeight = Number.parseFloat(computed?.lineHeight) || 24;
    const verticalPadding = (Number.parseFloat(computed?.paddingTop) || 0) +
      (Number.parseFloat(computed?.paddingBottom) || 0);
    textarea.rows = Math.min(10, Math.max(expanded ? 2 : 1, Math.ceil((textarea.scrollHeight - verticalPadding) / lineHeight)));
  };
  const expand = () => {
    form.classList.add("is-expanded");
    form.classList.remove("is-collapsed");
    resize();
  };

  form.classList.add("is-enhanced");
  if (form.querySelector(".reply-context, .quoted-post-preview")) form.classList.add("has-context");
  if (textarea.value || mediaInput?.files?.length) expand();
  else form.classList.add("is-collapsed");
  updateCounter();
  textarea.addEventListener("focus", expand);
  textarea.addEventListener("input", () => {
    updateCounter();
    if (textarea.value) expand();
    else resize();
  });
  mediaButton?.addEventListener("click", () => {
    expand();
    mediaInput?.click();
  });
  mediaInput?.addEventListener("change", () => {
    const selected = Boolean(mediaInput.files?.length);
    mediaPicker?.classList.toggle("has-media", selected);
    form.classList.toggle("has-open-field", selected || Boolean(linkPicker?.classList.contains("is-open")));
    if (selected) expand();
  });
  linkButton?.addEventListener("click", () => {
    const open = linkPicker?.classList.toggle("is-open") || false;
    linkButton.setAttribute("aria-expanded", String(open));
    form.classList.toggle("has-open-field", open || Boolean(mediaPicker?.classList.contains("has-media")));
    if (open) expand();
  });
  return { expand, resize, updateCounter };
}

function attachMediaComposer(form, helpers = {}) {
  const linkPreview = attachLinkPreview(form);
  const input = form.querySelector("[data-media-input]");
  const previews = form.querySelector("[data-media-previews]");
  const status = form.querySelector("[data-media-status]");
  const videoAltLabel = form.querySelector("[data-video-alt-label]");
  if (!input || !previews || !status) return null;

  const selected = [];
  const makeUrl = helpers.createObjectURL || ((file) => URL.createObjectURL(file));
  const revokeUrl = helpers.revokeObjectURL || ((url) => URL.revokeObjectURL(url));
  const canPostVideo = input.dataset?.videoAccess !== "false";
  const canPostImages = input.dataset?.imageAccess !== "false";
  const clearInput = () => {
    try {
      input.value = "";
    } catch {
      // File inputs can be read-only in lightweight DOM test doubles.
    }
  };
  const updatePreviewState = () => {
    const imageCount = selected.filter((item) => item.kind === "image").length;
    linkPreview.updateImagePrecedence(imageCount);
    if (videoAltLabel) videoAltLabel.hidden = !selected.some((item) => item.kind === "video");
  };
  const drawPreviews = () => {
    previews.replaceChildren();
    form.querySelector("[data-media-picker]")?.classList.toggle("has-media", selected.length > 0);
    selected.forEach((item, index) => {
      const preview = document.createElement("div");
      preview.className = item.kind === "video" ? "media-preview media-video-preview" : "media-preview image-preview";
      const details = document.createElement("div");
      if (item.kind === "video") {
        if (item.posterUrl) {
          const poster = document.createElement("img");
          poster.src = item.posterUrl;
          poster.alt = "Video preview frame";
          preview.append(poster);
        } else {
          const video = document.createElement("video");
          video.src = item.previewUrl;
          video.muted = true;
          video.playsInline = true;
          video.preload = "metadata";
          preview.append(video);
        }
        const summary = document.createElement("p");
        summary.className = "media-video-summary";
        summary.textContent = item.summary || `${new Intl.NumberFormat().format(item.file.size)} bytes · Reading duration…`;
        details.append(summary);
      } else {
        const image = document.createElement("img");
        image.src = item.previewUrl;
        image.alt = "";
        preview.append(image);
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
        details.append(altLabel);
      }
      const remove = document.createElement("button");
      remove.type = "button";
      remove.className = "media-remove";
      remove.textContent = "×";
      remove.setAttribute("aria-label", item.kind === "video" ? "Remove video" : `Remove image ${index + 1}`);
      remove.addEventListener("click", () => {
        revokeUrl(item.previewUrl);
        if (item.posterUrl) revokeUrl(item.posterUrl);
        selected.splice(index, 1);
        status.textContent = "";
        updatePreviewState();
        drawPreviews();
      });
      details.append(remove);
      preview.append(details);
      previews.append(preview);
    });
  };

  input.addEventListener("change", async () => {
    const files = [...(input.files || [])];
    clearInput();
    if (!files.length) return;
    const selection = selectComposerMedia(selected, files, canPostVideo, canPostImages);
    if (selection.kind === "error") {
      status.textContent = selection.message;
      return;
    }
    status.textContent = "";
    const added = selection.items.slice(selected.length);
    for (const item of added) {
      item.previewUrl = makeUrl(item.file);
      if (item.kind === "image") {
        item.alt = "";
      } else {
        item.mimeType = videoMimeType(item.file);
        item.valid = false;
        item.metadata = null;
        item.error = "Wait until video details are ready.";
        if (item.file.size > MAX_VIDEO_BYTES) {
          item.error = "Videos can be up to 300 MB. Choose a smaller file.";
          item.summary = `${new Intl.NumberFormat().format(item.file.size)} bytes · ${item.error}`;
        }
      }
      selected.push(item);
    }
    updatePreviewState();
    drawPreviews();
    const video = added.find((item) => item.kind === "video");
    if (video && video.file.size <= MAX_VIDEO_BYTES) {
      try {
        const metadata = await (helpers.probe || probeVideoMetadata)(video.previewUrl, helpers.createElement);
        if (selected[0] !== video) return;
        if (!Number.isFinite(metadata.duration) || metadata.duration <= 0) {
          throw new Error("The video duration could not be read.");
        }
        video.metadata = metadata;
        video.posterUrl = metadata.posterUrl;
        video.valid = metadata.duration <= MAX_VIDEO_DURATION_SECONDS && metadata.width > 0 && metadata.height > 0;
        video.error = metadata.duration > MAX_VIDEO_DURATION_SECONDS
          ? "Videos can be up to 10 minutes long."
          : "The video dimensions could not be read.";
        video.summary = `${new Intl.NumberFormat().format(video.file.size)} bytes · ${formatVideoDuration(metadata.duration)} · ${metadata.width}×${metadata.height}${video.valid ? "" : ` · ${video.error}`}`;
      } catch (reason) {
        if (selected[0] !== video) return;
        video.error = reason instanceof Error ? reason.message : "Video details could not be read.";
        video.summary = `${new Intl.NumberFormat().format(video.file.size)} bytes · ${video.error}`;
      }
      drawPreviews();
    }
  });

  form.addEventListener("submit", (event) => {
    const video = selected.find((item) => item.kind === "video");
    if (video) void handleVideoComposerSubmit(form, video, event, helpers);
    else if (selected.length) void handleImageComposerSubmit(form, selected, false, event, helpers);
  });
  return { selected };
}

(document.querySelectorAll?.("form[data-media-composer]") || []).forEach((form) => {
  enhanceCompactComposer(form);
  attachMediaComposer(form);
});

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
