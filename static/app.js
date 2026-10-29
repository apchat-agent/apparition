// Like toggles in place instead of reloading the page. Without this script the form posts as before.
document.addEventListener("submit", async (event) => {
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

(document.querySelectorAll?.("form[data-image-composer]") || []).forEach((form) => {
  const input = form.querySelector("[data-image-input]");
  const previews = form.querySelector("[data-image-previews]");
  const status = form.querySelector("[data-image-status]");
  if (!input || !previews || !status) return;

  const selected = [];
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
      const remove = document.createElement("button");
      remove.type = "button";
      remove.textContent = "Remove";
      remove.setAttribute("aria-label", `Remove image ${index + 1}`);
      remove.addEventListener("click", () => {
        URL.revokeObjectURL(item.previewUrl);
        selected.splice(index, 1);
        status.textContent = "";
        drawPreviews();
      });
      details.append(altLabel, remove);
      preview.append(image, details);
      previews.append(preview);
    });
  };

  input.addEventListener("change", () => {
    selected.forEach((item) => URL.revokeObjectURL(item.previewUrl));
    selected.length = 0;
    const files = [...(input.files || [])];
    if (files.length > 4) {
      status.textContent = "Choose no more than 4 images.";
      drawPreviews();
      return;
    }
    status.textContent = "";
    for (const file of files) {
      selected.push({ file, alt: "", previewUrl: URL.createObjectURL(file) });
    }
    drawPreviews();
  });

  form.addEventListener("submit", async (event) => {
    if (!selected.length) return;
    event.preventDefault();
    const button = form.querySelector('button[type="submit"]');
    const error = form.querySelector("[data-composer-error]");
    if (button?.disabled) return;
    if (button) button.disabled = true;
    error.hidden = true;
    error.textContent = "";
    try {
      const images = [];
      for (const item of selected) {
        const source = await loadComposerImage(item.file);
        const resized = await resizeImageCanvasSource(source);
        const filename = (item.file.name || "picture").replace(/\.[^.]*$/, "") + ".jpg";
        images.push({ ...resized, filename, alt: item.alt });
      }
      const data = new FormData(form);
      data.delete("images");
      data.delete("alts");
      for (const image of images) {
        data.append("images", new File([image.blob], image.filename, { type: "image/jpeg" }));
        data.append("alts", image.alt);
      }
      const response = await fetch(form.action, {
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
      window.location.assign(response.url || "/app");
    } catch (reason) {
      error.textContent = reason instanceof Error ? reason.message : "The post could not be created.";
      error.hidden = false;
    } finally {
      if (button) button.disabled = false;
    }
  });
});

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
