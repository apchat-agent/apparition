// @ts-nocheck

function assert(value, message = "assertion failed") {
  if (!value) throw new Error(message);
}

function assertEquals(actual, expected) {
  assert(actual === expected, `expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
}

Deno.test("empty composer status messages do not render status bars", async () => {
  const css = await Deno.readTextFile(new URL("../static/app.css", import.meta.url));
  assert(
    /\.composer\s+\[role="status"\]:empty\s*\{\s*display:\s*none;\s*\}/.test(css),
    "empty status elements inside the composer should be hidden",
  );
});

Deno.test("composer markup provides a compact control row and an icon-only link reveal", async () => {
  const source = await Deno.readTextFile(new URL("../src/oauth.rs", import.meta.url));
  const reply = source.slice(source.indexOf("fn render_reply_composer"), source.indexOf("fn render_media_picker"));
  assert(reply.includes("data-compose-input"), "reply text input should participate in compact composition");
  assert(reply.includes("data-compose-toolbar"), "reply composer should have a dedicated toolbar");
  assert(reply.includes("data-link-toggle"), "link entry should be opened from its toolbar icon");
  assert(reply.includes("data-character-count"), "reply composer should expose its character count");
  assert(source.includes("\"What's up?\""), "the main and quote fields should use the short placeholder");

  const css = await Deno.readTextFile(new URL("../static/app.css", import.meta.url));
  assert(css.includes(".composer.is-enhanced.is-collapsed .composer-toolbar"));
  assert(css.includes(".composer.is-enhanced .link-picker:not(.is-open)"));
});

Deno.test("composer expands, opens link entry, grows its text area, and warns near the limit", async () => {
  const source = await Deno.readTextFile(new URL("../static/app.js", import.meta.url));
  const start = source.indexOf("function enhanceCompactComposer");
  const end = source.indexOf("function attachMediaComposer", start);
  const enhance = new Function(`${source.slice(start, end)}; return enhanceCompactComposer;`)();
  const events = new Map();
  const makeClassList = () => {
    const names = new Set();
    return {
      add(name) { names.add(name); },
      remove(name) { names.delete(name); },
      contains(name) { return names.has(name); },
      toggle(name, force) {
        const enabled = force === undefined ? !names.has(name) : force;
        if (enabled) names.add(name);
        else names.delete(name);
        return enabled;
      },
    };
  };
  const textarea = {
    value: "",
    rows: 1,
    scrollHeight: 64,
    classList: makeClassList(),
    addEventListener(name, listener) { events.set(`textarea:${name}`, listener); },
  };
  const counter = { textContent: "", classList: makeClassList() };
  const mediaInput = {
    files: [],
    clicked: false,
    click() { this.clicked = true; },
    addEventListener(name, listener) { events.set(`media:${name}`, listener); },
  };
  const mediaPicker = { classList: makeClassList() };
  const linkPicker = { classList: makeClassList() };
  const linkButton = {
    attributes: new Map(),
    addEventListener(name, listener) { events.set(`link:${name}`, listener); },
    setAttribute(name, value) { this.attributes.set(name, value); },
  };
  const mediaButton = { addEventListener(name, listener) { events.set(`media-button:${name}`, listener); } };
  const form = {
    classList: makeClassList(),
    querySelector(selector) {
      return ({
        "[data-compose-input]": textarea,
        "[data-character-count]": counter,
        "[data-compose-media]": mediaButton,
        "[data-media-input]": mediaInput,
        "[data-media-picker]": mediaPicker,
        "[data-link-toggle]": linkButton,
        "[data-link-picker]": linkPicker,
        ".reply-context, .quoted-post-preview": null,
      })[selector] || null;
    },
  };
  const originalWindow = globalThis.window;
  globalThis.window = {
    getComputedStyle() { return { lineHeight: "24px", paddingTop: "8px", paddingBottom: "8px" }; },
  };
  try {
    enhance(form);
    assert(form.classList.contains("is-enhanced"));
    assert(form.classList.contains("is-collapsed"));
    assertEquals(textarea.rows, 1);

    events.get("textarea:focus")();
    assert(form.classList.contains("is-expanded"));
    assertEquals(textarea.rows, 2);

    events.get("link:click")();
    assert(linkPicker.classList.contains("is-open"));
    assertEquals(linkButton.attributes.get("aria-expanded"), "true");
    assert(form.classList.contains("has-open-field"));

    textarea.value = "x".repeat(275);
    textarea.scrollHeight = 88;
    events.get("textarea:input")();
    assertEquals(counter.textContent, "275/300");
    assert(counter.classList.contains("is-near-limit"));
    assertEquals(textarea.rows, 3);

    events.get("media-button:click")();
    assert(mediaInput.clicked);
    mediaInput.files = [{}];
    events.get("media:change")();
    assert(mediaPicker.classList.contains("has-media"));
  } finally {
    globalThis.window = originalWindow;
  }
});

Deno.test("phone navigation and post actions keep 44-pixel tap targets", async () => {
  const css = await Deno.readTextFile(new URL("../static/app.css", import.meta.url));
  const declarations = (selector) => {
    const escaped = selector.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    return [...css.matchAll(new RegExp(`${escaped}\\s*\\{([^}]*)\\}`, "g"))].map((match) => match[1]).at(-1) || "";
  };
  assert(/min-height:\s*44px/.test(declarations(".post-action")), "post action buttons should be at least 44 pixels tall");
  assert(css.includes(".feed-tabs,\n.feed-tab {\n  min-height: 2.75rem;"), "feed tabs should meet the 44-pixel target");
  assert(/body:has\(\.mobile-nav\) \.mobile-nav a,\s*body:has\(\.mobile-nav\) \.mobile-nav button\s*\{\s*min-height:\s*44px;/.test(css));
  assert(/width:\s*44px;[\s\S]*?height:\s*44px;/.test(declarations(".profile-overflow-trigger")));
  assert(/body:has\(\.mobile-nav\):has\(\.reply-composer\) \.mobile-compose\s*\{\s*display:\s*none;/.test(css), "the floating post button should not cover the thread reply composer");
});

Deno.test("the composer avatar keeps one size before and after app.js collapses it", async () => {
  const css = await Deno.readTextFile(new URL("../static/app.css", import.meta.url));
  const source = await Deno.readTextFile(new URL("../src/oauth.rs", import.meta.url));
  const ruleBody = (selector) => {
    const escaped = selector.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const rules = [...css.matchAll(new RegExp(`(?:^|\\n)[ \\t]*${escaped}[ \\t]*\\{([^}]*)\\}`, "g"))];
    return rules.map((match) => match[1]).at(-1) || "";
  };

  assert(
    source.includes('class=\\"composer-avatar\\"') && source.includes('width=\\"44\\" height=\\"44\\"'),
    "the server-rendered composer avatar must carry its 44 px size",
  );
  const base = ruleBody(".composer-avatar");
  assert(
    /width:\s*44px/.test(base) && /height:\s*44px/.test(base),
    "the .composer-avatar rule must keep the server-rendered 44 px size",
  );
  const collapsed = ruleBody(".composer.is-enhanced.is-collapsed .composer-avatar");
  assert(
    collapsed === "" || (/width:\s*44px/.test(collapsed) && /height:\s*44px/.test(collapsed)),
    "is-enhanced is-collapsed must not resize the composer avatar",
  );
  const columns = /grid-template-columns:\s*([^;]+)/.exec(ruleBody(".composer.is-enhanced.is-collapsed .composer-input-row"))?.[1]?.trim() ?? "";
  assert(columns === "" || columns.startsWith("44px"), "is-enhanced is-collapsed must not narrow the avatar column");
});

Deno.test("like handler fills the heart and clears it when unliked", async () => {
  const listeners = new Map();
  const documentMock = {
    addEventListener(name, listener) {
      listeners.set(name, listener);
    },
    querySelector() {
      return null;
    },
    querySelectorAll() {
      return [];
    },
    createElement(tag) {
      return {
        className: "",
        textContent: "",
        remove() {
          if (tag === "input") form.intent = null;
        },
      };
    },
  };
  const button = {
    disabled: false,
    attributes: new Map([
      ["aria-label", "Like post"],
      ["aria-pressed", "false"],
    ]),
    childNodes: [{ nodeType: 3, textContent: "Like" }],
    classes: new Set(),
    count: null,
    classList: {
      toggle(name, force) {
        if (force) button.classes.add(name);
        else button.classes.delete(name);
      },
      contains(name) {
        return button.classes.has(name);
      },
    },
    getAttribute(name) {
      return button.attributes.get(name) ?? null;
    },
    setAttribute(name, value) {
      button.attributes.set(name, value);
    },
    querySelector(selector) {
      return selector === ".action-count" ? button.count : null;
    },
    append(node) {
      if (node.className === "action-count") {
        node.remove = () => {
          button.count = null;
        };
        button.count = node;
      }
    },
  };
  class FakeFormElement {
    getAttribute(name) {
      return name === "action" ? "/like" : null;
    }
    querySelector(selector) {
      if (selector === "button") return button;
      if (selector === 'input[name="intent"]') return form.intent;
      return null;
    }
    append(input) {
      form.intent = input;
    }
    submit() {
      form.submitted = true;
    }
  }
  const form = new FakeFormElement();
  form.intent = null;
  form.submitted = false;
  let nextState = { liked: true, likeCount: 1 };

  globalThis.document = documentMock;
  globalThis.HTMLFormElement = FakeFormElement;
  globalThis.Node = { TEXT_NODE: 3 };
  globalThis.window = {
    scrollY: 0,
    scrollX: 0,
    visualViewport: { addEventListener() {} },
  };
  globalThis.FormData = class {
    [Symbol.iterator]() {
      return [][Symbol.iterator]();
    }
  };
  globalThis.fetch = async () => ({
    ok: true,
    json: async () => nextState,
  });

  const source = await Deno.readTextFile(new URL("../static/app.js", import.meta.url));
  new Function(source)();
  const submit = listeners.get("submit");
  assert(submit);

  await submit({ target: form, preventDefault() {} });
  assert(button.classList.contains("is-liked"), "like should fill the heart");
  assertEquals(button.getAttribute("aria-pressed"), "true");

  nextState = { liked: false, likeCount: 0 };
  await submit({ target: form, preventDefault() {} });
  assert(!button.classList.contains("is-liked"), "unlike should clear the heart");
  assertEquals(button.getAttribute("aria-pressed"), "false");
  assert(!form.submitted, "successful requests should keep using the in-place handler");
});

Deno.test("notification badges update from SSE and fall back to 30-second polling", async () => {
  const listeners = new Map();
  const visibilityListeners = new Map();
  const badges = [0, 1].map(() => ({
    hidden: true,
    textContent: "0",
    attributes: new Map(),
    setAttribute(name, value) {
      this.attributes.set(name, value);
    },
  }));
  const documentMock = {
    addEventListener(name, listener) {
      listeners.set(name, listener);
      if (name === "visibilitychange") visibilityListeners.set(name, listener);
    },
    querySelector() {
      return null;
    },
    querySelectorAll(selector) {
      return selector === "[data-notification-badge]" ? badges : [];
    },
  };
  const intervals = [];
  const requests = [];
  class FakeEventSource {
    constructor(url) {
      this.url = url;
      this.listeners = new Map();
      FakeEventSource.latest = this;
    }
    addEventListener(name, listener) {
      this.listeners.set(name, listener);
    }
    close() {
      this.closed = true;
    }
  }

  globalThis.document = documentMock;
  globalThis.HTMLFormElement = class {};
  globalThis.Node = { TEXT_NODE: 3 };
  globalThis.EventSource = FakeEventSource;
  globalThis.window = {
    scrollY: 0,
    scrollX: 0,
    EventSource: FakeEventSource,
    visualViewport: { addEventListener() {} },
    setInterval(callback, delay) {
      intervals.push({ callback, delay });
      return intervals.length;
    },
  };
  globalThis.fetch = async (url) => {
    requests.push(url);
    return { ok: true, json: async () => ({ count: 2 }) };
  };

  const source = await Deno.readTextFile(new URL("../static/app.js", import.meta.url));
  new Function(source)();
  const eventSource = FakeEventSource.latest;
  assertEquals(eventSource.url, "/notifications/stream");
  eventSource.listeners.get("unread")({ data: "10" });
  for (const badge of badges) {
    assertEquals(badge.textContent, "9+");
    assert(!badge.hidden);
    assertEquals(badge.attributes.get("aria-label"), "10 unread notifications");
  }
  eventSource.listeners.get("unread")({ data: "0" });
  assert(badges.every((badge) => badge.hidden));

  eventSource.onerror();
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert(eventSource.closed);
  assertEquals(intervals[0].delay, 30_000);
  assertEquals(requests[0], "/notifications/unread-count");
  visibilityListeners.get("visibilitychange")();
  await new Promise((resolve) => setTimeout(resolve, 0));
  assertEquals(requests.length, 2);
  assertEquals(badges[0].textContent, "2");
});

Deno.test("image canvas resize bounds JPEG bytes and dimensions", async () => {
  const source = await Deno.readTextFile(new URL("../static/app.js", import.meta.url));
  const resizeImageCanvasSource = new Function(`${source}; return resizeImageCanvasSource;`)();
  const draws = [];
  globalThis.document = {
    querySelectorAll() {
      return [];
    },
    createElement(tag) {
      assertEquals(tag, "canvas");
      return {
        width: 0,
        height: 0,
        getContext() {
          return {
            drawImage(image, x, y, width, height) {
              draws.push({ image, x, y, width, height });
            },
          };
        },
        toBlob(callback, mime, quality) {
          assertEquals(mime, "image/jpeg");
          const bytes = Math.ceil(this.width * this.height * quality * 0.12);
          callback(new Blob([new Uint8Array(bytes)], { type: mime }));
        },
      };
    },
  };

  const result = await resizeImageCanvasSource({ width: 6000, height: 3000 });
  assert(result.blob.size <= 950_000, `expected <= 950000 bytes, got ${result.blob.size}`);
  assertEquals(result.blob.type, "image/jpeg");
  assert(Math.max(result.width, result.height) <= 2000);
  assertEquals(result.width, 2000);
  assertEquals(result.height, 1000);
  assert(draws.length > 0);
});

Deno.test("video selection metadata and upload progress use the dedicated video route", async () => {
  const source = await Deno.readTextFile(new URL("../static/app.js", import.meta.url));
  globalThis.document = {
    addEventListener() {},
    querySelector() { return null; },
    querySelectorAll() { return []; },
  };
  globalThis.window = { visualViewport: { addEventListener() {} }, scrollY: 0, scrollX: 0 };
  const helpers = new Function(`${source}; return { videoMimeType, formatVideoDuration, handleVideoComposerSubmit };`)();
  assertEquals(helpers.videoMimeType({ type: "", name: "clip.mov" }), "video/quicktime");
  assertEquals(helpers.formatVideoDuration(71.9), "1:11");

  const button = { disabled: false };
  const error = { hidden: true, textContent: "" };
  const status = { textContent: "" };
  const progress = { hidden: true, max: 0, value: 0 };
  const alt = { value: "A cyclist crosses the road" };
  const form = {
    querySelector(selector) {
      return ({
        'button[type="submit"]': button,
        "[data-composer-error]": error,
        "[data-video-upload-status]": status,
        "[data-video-progress]": progress,
        "[data-video-alt]": alt,
      })[selector] || null;
    },
  };
  class FakeFormData {
    values = new Map();
    constructor() {}
    delete(name) { this.values.delete(name); }
    set(name, value) { this.values.set(name, value); }
    get(name) { return this.values.get(name); }
  }
  class FakeFile {
    constructor(parts, name, options) {
      this.parts = parts;
      this.name = name;
      this.type = options.type;
    }
  }
  class FakeXmlHttpRequest {
    upload = {};
    open(method, url) { this.method = method; this.url = url; }
    send(body) {
      this.body = body;
      this.upload.onprogress({ lengthComputable: true, loaded: 42, total: 42 });
      this.status = 200;
      this.responseURL = "/app";
      this.onload();
    }
  }
  let destination;
  const selected = {
    file: { name: "clip.mp4", size: 42 },
    mimeType: "video/mp4",
    valid: true,
    metadata: { duration: 12.5, width: 640, height: 360 },
  };
  let prevented = false;
  await helpers.handleVideoComposerSubmit(
    form,
    selected,
    { preventDefault() { prevented = true; } },
    {
      FormData: FakeFormData,
      File: FakeFile,
      XMLHttpRequest: FakeXmlHttpRequest,
      navigate(url) { destination = url; },
    },
  );
  assert(prevented);
  assertEquals(destination, "/app");
  assertEquals(status.textContent, "Video received. Bluesky is processing it…");
  assertEquals(progress.value, 100);
  assertEquals(error.hidden, true);
  assertEquals(button.disabled, false);
});

Deno.test("unified media selection routes mixed, image-only, one-video, and two-video picks", async () => {
  const source = await Deno.readTextFile(new URL("../static/app.js", import.meta.url));
  globalThis.document = { addEventListener() {}, querySelector() { return null; }, querySelectorAll() { return []; } };
  globalThis.window = { visualViewport: { addEventListener() {} }, scrollY: 0, scrollX: 0 };
  const selectMedia = new Function(`${source}; return selectComposerMedia;`)();
  const file = (name, type) => ({ name, type });

  const mixed = selectMedia([], [file("cat.jpg", "image/jpeg"), file("clip.mp4", "video/mp4")]);
  assertEquals(mixed.kind, "error");
  assert(mixed.message.includes("either images or one video"));
  assertEquals(mixed.items.length, 0);

  const images = selectMedia([], [file("cat.jpg", "image/jpeg"), file("dog.png", "image/png")]);
  assertEquals(images.kind, "images");
  assertEquals(images.items.length, 2);

  const video = selectMedia([], [file("clip.mp4", "video/mp4")]);
  assertEquals(video.kind, "video");
  assertEquals(video.items.length, 1);

  const twoVideos = selectMedia([], [file("one.mp4", "video/mp4"), file("two.mov", "video/quicktime")]);
  assertEquals(twoVideos.kind, "error");
  assert(twoVideos.message.includes("one video"));
});

Deno.test("unified media selection keeps earlier media when a later pick conflicts or lacks video scope", async () => {
  const source = await Deno.readTextFile(new URL("../static/app.js", import.meta.url));
  globalThis.document = { addEventListener() {}, querySelector() { return null; }, querySelectorAll() { return []; } };
  globalThis.window = { visualViewport: { addEventListener() {} }, scrollY: 0, scrollX: 0 };
  const selectMedia = new Function(`${source}; return selectComposerMedia;`)();
  const image = { name: "cat.jpg", type: "image/jpeg" };
  const video = { name: "clip.mp4", type: "video/mp4" };

  const conflict = selectMedia([{ file: image, kind: "image" }], [video]);
  assertEquals(conflict.kind, "error");
  assert(conflict.message.includes("Remove the selected images"));
  assertEquals(conflict.items.length, 1);
  assertEquals(conflict.items[0].file, image);

  const oldScope = selectMedia([], [video], false);
  assertEquals(oldScope.kind, "error");
  assertEquals(oldScope.message, "sign in again to post videos");

  const reverseConflict = selectMedia([{ file: video, kind: "video" }], [image]);
  assertEquals(reverseConflict.kind, "error");
  assert(reverseConflict.message.includes("Remove the selected video"));
  assertEquals(reverseConflict.items[0].file, video);

  const secondVideo = selectMedia([{ file: video, kind: "video" }], [{ name: "second.mp4", type: "video/mp4" }]);
  assertEquals(secondVideo.kind, "error");
  assert(secondVideo.message.includes("one video"));
});

Deno.test("each selected image renders a manual alt text field", async () => {
  const source = await Deno.readTextFile(new URL("../static/app.js", import.meta.url));
  const makeElement = (tag) => ({
    tag,
    children: [],
    attributes: {},
    listeners: new Map(),
    addEventListener(name, listener) { this.listeners.set(name, listener); },
    append(...nodes) { this.children.push(...nodes); },
    replaceChildren() { this.children = []; },
    setAttribute(name, value) { this.attributes[name] = value; },
  });
  const input = makeElement("input");
  input.files = [Object.assign(new Blob(["picture"]), { name: "cat.png" })];
  const previews = makeElement("previews");
  const status = makeElement("status");
  const linkInput = makeElement("input");
  linkInput.value = "https://example.test/article";
  const linkButton = makeElement("button");
  const linkStatus = makeElement("status");
  const linkPreview = makeElement("preview");
  const linkImageNote = makeElement("notice");
  const form = {
    querySelector(selector) {
      if (selector === "[data-media-input]") return input;
      if (selector === "[data-media-previews]") return previews;
      if (selector === "[data-media-status]") return status;
      if (selector === "[data-link-input]") return linkInput;
      if (selector === "[data-link-preview]") return linkButton;
      if (selector === "[data-link-status]") return linkStatus;
      if (selector === "[data-link-preview-card]") return linkPreview;
      if (selector === "[data-link-image-note]") return linkImageNote;
      return null;
    },
    addEventListener() {},
  };
  globalThis.document = {
    addEventListener() {},
    createElement: makeElement,
    querySelector() { return null; },
    querySelectorAll(selector) { return selector === "form[data-media-composer]" ? [form] : []; },
  };
  globalThis.window = { visualViewport: { addEventListener() {} } };
  new Function(source)();
  input.listeners.get("change")();

  const preview = previews.children[0];
  assert(preview, "choosing a picture should create its preview");
  assert(!linkImageNote.hidden, "pictures and a link should show the precedence notice");
  const details = preview.children[1];
  const altLabel = details.children.find((child) => child.tag === "label");
  assert(altLabel, "the image preview should have a manual alt text label");
  assertEquals(altLabel.textContent, "Alt text for image 1 (optional)");
  const altInput = altLabel.children[0];
  assertEquals(altInput.tag, "textarea");
  assertEquals(altInput.name, "alts");
  assertEquals(altInput.maxLength, 2000);
  const remove = details.children.find((child) => child.tag === "button" && child.attributes["aria-label"] === "Remove image 1");
  remove.listeners.get("click")();
  assert(linkImageNote.hidden, "removing the last picture should clear the precedence notice");
});

Deno.test("composer link preview is removable and renders metadata as text", async () => {
  const source = await Deno.readTextFile(new URL("../static/app.js", import.meta.url));
  const makeElement = (tag) => ({
    tag,
    children: [],
    listeners: new Map(),
    attributes: new Map(),
    hidden: false,
    textContent: "",
    addEventListener(name, listener) { this.listeners.set(name, listener); },
    append(...nodes) { this.children.push(...nodes); },
    replaceChildren() { this.children = []; },
    setAttribute(name, value) { this.attributes.set(name, value); },
    click() { return this.listeners.get("click")?.({}); },
  });
  globalThis.document = {
    addEventListener() {},
    createElement: makeElement,
    querySelector() { return null; },
    querySelectorAll() { return []; },
  };
  globalThis.window = { visualViewport: { addEventListener() {} } };
  const attachLinkPreview = new Function(`${source}; return attachLinkPreview;`)();
  const input = makeElement("input");
  input.value = "https://example.test/article";
  const button = makeElement("button");
  const status = makeElement("status");
  const preview = makeElement("preview");
  const imageNote = makeElement("note");
  const form = {
    querySelector(selector) {
      return ({
        "[data-link-input]": input,
        "[data-link-preview]": button,
        "[data-link-status]": status,
        "[data-link-preview-card]": preview,
        "[data-link-image-note]": imageNote,
      })[selector] || null;
    },
  };
  let request;
  const linkPicker = attachLinkPreview(form, {
    fetch: async (url, options) => {
      request = { url, options };
      return {
        ok: true,
        json: async () => ({
          uri: input.value,
          title: "A <title> from the page",
          description: "A useful summary",
          thumbnail: null,
        }),
      };
    },
  });

  await button.click();
  assertEquals(request.url, "/link-preview");
  assertEquals(new URLSearchParams(request.options.body).get("url"), input.value);
  assertEquals(request.options.credentials, "same-origin");
  assert(!preview.hidden);
  const card = preview.children[0];
  assertEquals(card.children[0].textContent, "A <title> from the page");
  assertEquals(card.children[1].textContent, "A useful summary");
  assertEquals(status.textContent, "Webpage preview ready.");

  linkPicker.updateImagePrecedence(1);
  assert(!imageNote.hidden, "the composer should explain that pictures take precedence");
  await card.children[3].click();
  assertEquals(input.value, "");
  assert(preview.hidden);
  assertEquals(status.textContent, "Webpage removed.");
});

Deno.test("infinite scroll loads once past the newest page midpoint in the pinned phone frame", async () => {
  const source = await Deno.readTextFile(new URL("../static/app.js", import.meta.url));
  const setupSourceEnd = source.indexOf("\ninstallInfiniteScroll();");
  assert(setupSourceEnd > 0, "app.js should automatically install infinite scroll after defining it");
  const setupSource = source.slice(0, setupSourceEnd);

  class Element {
    constructor(tag = "div", uri = "") {
      this.tag = tag;
      this.children = [];
      this.attributes = new Map(uri ? [["data-post-uri", uri]] : []);
      this.listeners = new Map();
      this.className = "";
      this.textContent = "";
      this.top = 100;
      this.parentElement = null;
      this.removed = false;
    }
    append(...nodes) {
      for (const node of nodes) {
        node.parentElement = this;
        this.children.push(node);
      }
    }
    after(...nodes) { this.afterNodes = [...(this.afterNodes || []), ...nodes]; }
    addEventListener(name, listener) { this.listeners.set(name, listener); }
    setAttribute(name, value) { this.attributes.set(name, String(value)); }
    getAttribute(name) { return this.attributes.get(name) ?? null; }
    remove() {
      this.removed = true;
      if (this.parentElement) this.parentElement.children = this.parentElement.children.filter((child) => child !== this);
    }
    getBoundingClientRect() { return { top: this.top }; }
  }
  class Link extends Element {
    constructor(href) { super("a"); this.href = href; }
  }
  class Observer {
    constructor(callback, options) {
      this.callback = callback;
      this.options = options;
      Observer.latest = this;
    }
    observe(target) { this.target = target; }
    unobserve(target) { if (this.target === target) this.target = null; }
    disconnect() { this.target = null; }
    fire(isIntersecting = true) {
      this.callback([{ target: this.target, isIntersecting }]);
    }
  }

  const initialCards = ["post-1", "post-2", "post-3", "post-4"].map((uri) => new Element("li", uri));
  const list = new Element("ol");
  list.className = "post-list";
  list.append(...initialCards);
  const layout = new Element("div");
  layout.top = 0;
  const link = new Link("/older-a");
  const documentMock = {
    addEventListener() {},
    querySelector(selector) {
      if (selector === ".pagination-link[rel=\"next\"]") return link;
      if (selector === ".post-list, .notification-list") return list;
      if (selector === ".app-layout") return layout;
      if (selector === ".mobile-nav") return {};
      return null;
    },
    querySelectorAll() { return []; },
    createElement(tag) { return new Element(tag); },
  };
  const windowMock = {
    visualViewport: { addEventListener() {} },
    matchMedia() { return { matches: true }; },
    addEventListener(name, listener) { this.listeners.set(name, listener); },
    listeners: new Map(),
    EventSource: class {
      addEventListener() {}
    },
  };
  globalThis.document = documentMock;
  globalThis.window = windowMock;
  globalThis.EventSource = windowMock.EventSource;
  globalThis.HTMLFormElement = class {};
  globalThis.Node = { TEXT_NODE: 3 };

  let releaseFetch;
  const pendingResponse = new Promise((resolve) => { releaseFetch = resolve; });
  const requests = [];
  const duplicatePost = new Element("li", "post-2");
  duplicatePost.setAttribute("data-post-kind", "post");
  const fifthPost = new Element("li", "post-5");
  fifthPost.setAttribute("data-post-kind", "post");
  const replyPost = new Element("li", "post-6");
  replyPost.setAttribute("data-post-kind", "post");
  const duplicateParent = new Element("a", "post-1");
  duplicateParent.setAttribute("data-post-kind", "parent");
  replyPost.append(duplicateParent);
  const nextCards = [duplicatePost, fifthPost, replyPost];
  const nextList = new Element("ol");
  nextList.className = "post-list";
  nextList.append(...nextCards);
  let parsedPage;
  const installInfiniteScroll = new Function(`${setupSource}; return installInfiniteScroll;`)();
  assertEquals(typeof installInfiniteScroll, "function", "app.js should expose its infinite-scroll installer");
  installInfiniteScroll({
    fetchPage: (url) => {
      requests.push(url);
      return pendingResponse;
    },
    parsePage: () => parsedPage,
    Observer,
  });

  assertEquals(Observer.latest.options.root, layout, "the narrow pinned app should observe its real scroll container");
  Observer.latest.fire();
  assertEquals(requests.length, 0, "the sentinel must not fetch before the middle of the page");
  Observer.latest.target.top = 1; // 49%: the middle card has not crossed the container's top edge.
  layout.listeners.get("scroll")();
  assertEquals(requests.length, 0, "49% of the newest page must not fetch");

  Observer.latest.target.top = -1; // 51%: the middle card has crossed the container's top edge.
  layout.listeners.get("scroll")();
  await Promise.resolve();
  assertEquals(requests.length, 1, "51% of the newest page should start one request");
  assertEquals(requests[0], "/older-a");
  Observer.latest.fire();
  Observer.latest.fire();
  assertEquals(requests.length, 1, "repeated observer notifications during a request must not duplicate it");

  parsedPage = { list: nextList, link: null };
  releaseFetch({ ok: true, text: async () => "page-two" });
  await new Promise((resolve) => setTimeout(resolve, 0));
  assertEquals(list.children.map((card) => card.getAttribute("data-post-uri")).join(","), "post-1,post-2,post-3,post-4,post-5,post-6");
  assert(duplicatePost.removed, "a repeated post URI must be skipped across pages");
  assert(duplicateParent.removed, "a reply parent already on the page must not be repeated");
  Observer.latest.fire();
  layout.listeners.get("scroll")();
  assertEquals(requests.length, 1, "the end of the list must stop future requests");
  assert((link.afterNodes || []).some((node) => node.textContent.includes("caught up")), "the end should have a readable status");
});

async function createInfiniteScrollHarness({ cards = ["post-1", "post-2"], mobile = false, responses = [], pages = {} } = {}) {
  class Element {
    constructor(tag = "div", uri = "") {
      this.tag = tag;
      this.children = [];
      this.attributes = new Map(uri ? [["data-post-uri", uri]] : []);
      this.listeners = new Map();
      this.className = "";
      this.textContent = "";
      this.top = 100;
      this.parentElement = null;
      this.removed = false;
      this.hidden = false;
    }
    querySelectorAll(selector) {
      const matches = [];
      const attribute = /^\[data-[\w-]+\]$/.test(selector) ? selector.slice(1, -1) : null;
      const visit = (node) => {
        for (const child of node.children || []) {
          if (selector === "video[data-video-playlist]" && child.tagName === "VIDEO" && child.getAttribute("data-video-playlist")) {
            matches.push(child);
          }
          if (attribute && child.getAttribute?.(attribute) !== null) {
            matches.push(child);
          }
          visit(child);
        }
      };
      visit(this);
      return matches;
    }
    append(...nodes) {
      for (const node of nodes) {
        node.parentElement = this;
        this.children.push(node);
      }
    }
    insertBefore(node, reference) {
      const index = this.children.indexOf(reference);
      node.parentElement = this;
      this.children.splice(index < 0 ? this.children.length : index, 0, node);
    }
    after(...nodes) { this.afterNodes = [...(this.afterNodes || []), ...nodes]; }
    addEventListener(name, listener) { this.listeners.set(name, listener); }
    setAttribute(name, value) { this.attributes.set(name, String(value)); }
    getAttribute(name) { return this.attributes.get(name) ?? null; }
    remove() {
      this.removed = true;
      if (this.parentElement) this.parentElement.children = this.parentElement.children.filter((child) => child !== this);
    }
    getBoundingClientRect() { return { top: this.top }; }
  }
  class Link extends Element {
    constructor(href) { super("a"); this.href = href; this.setAttribute("href", href); }
  }
  class Observer {
    constructor(callback, options) {
      this.callback = callback;
      this.options = options;
      Observer.latest = this;
    }
    observe(target) { this.target = target; }
    unobserve(target) { if (this.target === target) this.target = null; }
    disconnect() { this.target = null; }
    fire() { this.callback([{ target: this.target, isIntersecting: true }]); }
  }
  const list = new Element("ol");
  list.className = "post-list";
  list.append(...cards.map((uri) => new Element("li", uri)));
  const layout = new Element("div");
  layout.top = 0;
  const link = new Link("/older-a");
  const windowListeners = new Map();
  const documentListeners = new Map();
  const documentMock = {
    addEventListener(name, listener) { documentListeners.set(name, listener); },
    querySelector(selector) {
      if (selector === '.pagination-link[rel="next"]') return link;
      if (selector === ".post-list, .notification-list") return list;
      if (selector === ".app-layout") return layout;
      if (selector === ".mobile-nav") return mobile ? {} : null;
      return null;
    },
    querySelectorAll() { return []; },
    createElement(tag) { return new Element(tag); },
  };
  const windowMock = {
    visualViewport: { addEventListener() {} },
    matchMedia() { return { matches: mobile }; },
    addEventListener(name, listener) { windowListeners.set(name, listener); },
    listeners: windowListeners,
    EventSource: class { addEventListener() {} },
  };
  globalThis.document = documentMock;
  globalThis.window = windowMock;
  globalThis.EventSource = windowMock.EventSource;
  globalThis.HTMLFormElement = class {};
  globalThis.Node = { TEXT_NODE: 3 };
  const source = await Deno.readTextFile(new URL("../static/app.js", import.meta.url));
  const setupSourceEnd = source.indexOf("\ninstallInfiniteScroll();");
  const installInfiniteScroll = new Function(`${source.slice(0, setupSourceEnd)}; return installInfiniteScroll;`)();
  const requests = [];
  installInfiniteScroll({
    pageDocument: documentMock,
    pageWindow: windowMock,
    fetchPage: (href) => {
      requests.push(href);
      const response = responses[requests.length - 1];
      return typeof response === "function" ? response() : response;
    },
    parsePage: (html) => pages[html],
    Observer,
  });
  const status = link.afterNodes[0];
  const retry = link.afterNodes[1];
  return {
    Element,
    Link,
    Observer,
    documentMock,
    documentListeners,
    layout,
    link,
    list,
    requests,
    retry,
    scroll: () => (mobile ? layout : windowMock).listeners.get("scroll")(),
    status,
    windowMock,
  };
}

Deno.test("case33: a video in an appended card gets a native HLS source", async () => {
  const pages = {};
  const harness = await createInfiniteScrollHarness({
    responses: [{ ok: true, text: async () => "page-two" }],
    pages,
  });
  const card = new harness.Element("li", "post-video");
  const playlist = "https://video.bsky.app/watch/did%3Aplc%3Atest/cid/playlist.m3u8";
  const video = {
    tagName: "VIDEO",
    dataset: {},
    attributes: new Map([["data-video-playlist", playlist]]),
    src: "",
    getAttribute(name) { return this.attributes.get(name) ?? null; },
    canPlayType(type) { return type === "application/vnd.apple.mpegurl" ? "maybe" : ""; },
  };
  card.append(video);
  const nextList = new harness.Element("ol");
  nextList.className = "post-list";
  nextList.append(card);
  pages["page-two"] = { list: nextList, link: null };

  harness.Observer.latest.target.top = -1;
  harness.scroll();
  await new Promise((resolve) => setTimeout(resolve, 0));

  assertEquals(video.src, playlist, "native-HLS browsers need a playlist source on appended videos");
  assertEquals(video.dataset.videoInitialized, "true", "the appended video should initialize only once");
});

Deno.test("case33: a pending quote replaces its loading placeholder with the fetched card", async () => {
  const source = await Deno.readTextFile(new URL("../static/app.js", import.meta.url));
  const start = source.indexOf("function fillDeferredQuotes(");
  const end = source.indexOf("\nfillDeferredQuotes();", start);
  assert(start >= 0 && end > start, "app.js should install deferred quote fills");
  const fillDeferredQuotes = new Function(`${source.slice(start, end)}; return fillDeferredQuotes;`)();
  const card = {
    dataset: {},
    matches() { return false; },
    querySelectorAll() { return []; },
  };
  const placeholder = {
    dataset: {},
    matches(selector) { return selector === "[data-quote-fragment]"; },
    querySelectorAll() { return []; },
    getAttribute(name) {
      return ({
        "data-quote-fragment": "/quote-fragment?uri=at%3A%2F%2Fdid%3Aplc%3Aauthor%2Fapp.bsky.feed.post%2Fpost",
      })[name] ?? null;
    },
    replaceWith(node) { this.replacedWith = node; },
  };
  let request;
  const originalFetch = globalThis.fetch;
  const originalDocument = globalThis.document;
  const originalDOMParser = globalThis.DOMParser;
  globalThis.document = { createElement() { return {}; } };
  globalThis.DOMParser = class {
    parseFromString(html) {
      assert(html.includes("quoted-post-card"));
      return { querySelector(selector) { return selector === ".quoted-post-card" ? card : null; } };
    }
  };
  globalThis.fetch = async (url, options) => {
    request = { url, options };
    return { ok: true, text: async () => "<article class=\"quoted-post-card\">Loaded quote</article>" };
  };
  try {
    fillDeferredQuotes(placeholder);
    await new Promise((resolve) => setTimeout(resolve, 0));
    assertEquals(request.url, placeholder.getAttribute("data-quote-fragment"));
    assertEquals(request.options.credentials, "same-origin");
    assertEquals(request.options.headers.Accept, "text/html");
    assertEquals(placeholder.replacedWith, card);
  } finally {
    globalThis.fetch = originalFetch;
    globalThis.document = originalDocument;
    globalThis.DOMParser = originalDOMParser;
  }
});

Deno.test("infinite scroll errors wait for an explicit retry and do not storm requests", async () => {
  const pages = {};
  const harness = await createInfiniteScrollHarness({
    responses: [
      { ok: false, status: 503 },
      { ok: true, text: async () => "page-two" },
    ],
    pages,
  });
  const nextList = new harness.Element("ol");
  nextList.className = "post-list";
  nextList.append(new harness.Element("li", "post-3"));
  pages["page-two"] = { list: nextList, link: null };
  harness.Observer.latest.target.top = -1;
  harness.scroll();
  await new Promise((resolve) => setTimeout(resolve, 0));
  assertEquals(harness.requests.length, 1);
  assert(harness.status.textContent.includes("Couldn’t load"));
  assert(!harness.retry.hidden, "an error should reveal a retry button");
  harness.Observer.latest.fire();
  harness.scroll();
  await Promise.resolve();
  assertEquals(harness.requests.length, 1, "observer events after an error must not retry automatically");
  harness.retry.listeners.get("click")();
  await new Promise((resolve) => setTimeout(resolve, 0));
  assertEquals(harness.requests.length, 2, "one deliberate retry should make one request");
  assert(harness.link.removed, "a successful retry that reaches the end should remove the Older link");
  assertEquals(harness.list.children.at(-1).getAttribute("data-post-uri"), "post-3");
});

Deno.test("an incoming full post keeps an earlier reply parent card on screen", async () => {
  const pages = {};
  const harness = await createInfiniteScrollHarness({
    cards: ["reply-R"],
    responses: [{ ok: true, text: async () => "page-two" }],
    pages,
  });
  const reply = harness.list.children[0];
  reply.setAttribute("data-post-kind", "post");
  const parent = new harness.Element("a", "post-P");
  parent.setAttribute("data-post-kind", "parent");
  reply.append(parent);

  const nextList = new harness.Element("ol");
  nextList.className = "post-list";
  const fullParentPost = new harness.Element("li", "post-P");
  fullParentPost.setAttribute("data-post-kind", "post");
  nextList.append(fullParentPost);
  pages["page-two"] = { list: nextList, link: new harness.Link("/older-b") };

  harness.Observer.latest.target.top = -1;
  harness.scroll();
  await new Promise((resolve) => setTimeout(resolve, 0));

  assert(!parent.removed, "the earlier reply parent card must remain on screen");
  assert(reply.children.includes(parent), "the parent card must stay attached to its original reply");
  assertEquals(
    harness.list.children.filter((card) =>
      card.getAttribute("data-post-uri") === "post-P" && card.getAttribute("data-post-kind") !== "parent"
    ).length,
    1,
    "the full parent post should appear once",
  );
});

Deno.test("infinite scroll merges a new reply into an on-screen thread without removing cards", async () => {
  const pages = {};
  const harness = await createInfiniteScrollHarness({
    cards: ["seed"],
    responses: [{ ok: true, text: async () => "page-two" }],
    pages,
  });
  harness.list.children[0].remove();
  const existingGroup = new harness.Element("li");
  existingGroup.className = "post-item thread-group";
  const postA = new harness.Element("article");
  postA.setAttribute("data-post-kind", "post");
  postA.setAttribute("data-post-uri", "thread-A");
  const postB = new harness.Element("article");
  postB.setAttribute("data-post-kind", "post");
  postB.setAttribute("data-post-uri", "thread-B");
  const threadMore = new harness.Element("a");
  threadMore.className = "thread-more";
  threadMore.textContent = "2 more replies";
  existingGroup.append(postA, postB, threadMore);
  harness.list.append(existingGroup);

  const nextList = new harness.Element("ol");
  nextList.className = "post-list";
  const incomingGroup = new harness.Element("li");
  incomingGroup.className = "post-item thread-group";
  const duplicateB = new harness.Element("article");
  duplicateB.setAttribute("data-post-kind", "post");
  duplicateB.setAttribute("data-post-uri", "thread-B");
  const postC = new harness.Element("article");
  postC.setAttribute("data-post-kind", "post");
  postC.setAttribute("data-post-uri", "thread-C");
  incomingGroup.append(duplicateB, postC);
  nextList.append(incomingGroup);
  pages["page-two"] = { list: nextList, link: null };

  harness.Observer.latest.target.top = -1;
  harness.scroll();
  await new Promise((resolve) => setTimeout(resolve, 0));

  assert(!existingGroup.removed, "the on-screen group must remain attached");
  assert(harness.list.children.includes(existingGroup));
  assert(existingGroup.children.includes(postC), "the new reply should join the on-screen group");
  assertEquals(threadMore.textContent, "1 more reply");
  assert(
    existingGroup.children.indexOf(postC) < existingGroup.children.indexOf(threadMore),
    "the more link should stay after the newly loaded reply",
  );
  const uriCounts = new Map();
  const collect = (node) => {
    const uri = node.getAttribute?.("data-post-uri");
    if (uri) uriCounts.set(uri, (uriCounts.get(uri) || 0) + 1);
    for (const child of node.children || []) collect(child);
  };
  collect(harness.list);
  assertEquals(uriCounts.get("thread-A"), 1);
  assertEquals(uriCounts.get("thread-B"), 1);
  assertEquals(uriCounts.get("thread-C"), 1);
});

Deno.test("infinite scroll stops before retaining more than 500 cards", async () => {
  const harness = await createInfiniteScrollHarness({
    cards: Array.from({ length: 500 }, (_, index) => `post-${index}`),
  });
  harness.Observer.latest.target.top = -1;
  harness.scroll();
  await new Promise((resolve) => setTimeout(resolve, 0));
  assertEquals(harness.requests.length, 0, "the 500-card cap should prevent another page request");
  assertEquals(harness.list.children.length, 500);
  assert(!harness.link.removed, "the Older link should remain available to continue with a full navigation");
  assert(harness.status.textContent.includes("500"), "the status should explain why automatic loading stopped");

  const pages = {};
  const nearLimit = await createInfiniteScrollHarness({
    cards: Array.from({ length: 499 }, (_, index) => `near-limit-${index}`),
    responses: [{ ok: true, text: async () => "oversized-page" }],
    pages,
  });
  const nextList = new nearLimit.Element("ol");
  nextList.className = "post-list";
  nextList.append(new nearLimit.Element("li", "post-501"), new nearLimit.Element("li", "post-502"));
  pages["oversized-page"] = {
    list: nextList,
    link: new nearLimit.Link("/older-b"),
  };
  nearLimit.Observer.latest.target.top = -1;
  nearLimit.scroll();
  await new Promise((resolve) => setTimeout(resolve, 0));
  assertEquals(nearLimit.list.children.length, 499, "a fetched page must not push retained cards over the cap");
  assert(nearLimit.status.textContent.includes("499"), "the status should report how many cards remain in memory");
  assertEquals(nearLimit.link.getAttribute("href"), "/older-a", "the fallback link should still point to this page");
});

Deno.test("a like form on an appended card uses the existing delegated handler", async () => {
  const pages = {};
  const harness = await createInfiniteScrollHarness({
    responses: [{ ok: true, text: async () => "page-two" }],
    pages,
  });
  const nextList = new harness.Element("ol");
  nextList.className = "post-list";
  const card = new harness.Element("li", "post-3");
  const form = new globalThis.HTMLFormElement();
  const button = {
    disabled: false,
    attributes: new Map([["aria-label", "Like post"], ["aria-pressed", "false"]]),
    childNodes: [{ nodeType: 3, textContent: "Like" }],
    classes: new Set(),
    classList: {
      toggle(name, force) { force ? button.classes.add(name) : button.classes.delete(name); },
    },
    getAttribute(name) { return button.attributes.get(name) ?? null; },
    setAttribute(name, value) { button.attributes.set(name, value); },
    querySelector() { return null; },
    append() {},
  };
  form.getAttribute = (name) => name === "action" ? "/like" : null;
  form.querySelector = (selector) => selector === "button" ? button : null;
  form.append = () => {};
  form.submit = () => { form.submitted = true; };
  card.append(form);
  nextList.append(card);
  pages["page-two"] = { list: nextList, link: null };

  const originalFormData = globalThis.FormData;
  const originalFetch = globalThis.fetch;
  globalThis.FormData = class {
    [Symbol.iterator]() { return [][Symbol.iterator](); }
  };
  let likeRequests = 0;
  globalThis.fetch = async (url) => {
    assertEquals(url, "/like");
    likeRequests++;
    return { ok: true, json: async () => ({ liked: true, likeCount: 1 }) };
  };
  try {
    harness.Observer.latest.target.top = -1;
    harness.scroll();
    await new Promise((resolve) => setTimeout(resolve, 0));
    let prevented = false;
    await harness.documentListeners.get("submit")({ target: form, preventDefault() { prevented = true; } });
    assert(prevented, "the delegated like handler should intercept an appended form");
    assertEquals(likeRequests, 1);
    assert(button.classes.has("is-liked"), "the appended card's heart should update in place");
  } finally {
    globalThis.FormData = originalFormData;
    globalThis.fetch = originalFetch;
  }
});

function createProfileFillHarness() {
  const makeElement = ({ did = "", avatar = false, name = false, handle = false, tag = "SPAN" } = {}) => ({
    tagName: tag.toUpperCase(),
    className: avatar ? "avatar avatar-placeholder" : "",
    dataset: {},
    attributes: new Map([
      ...(did ? [["data-profile-did", did]] : []),
      ...(avatar ? [["data-profile-avatar", ""]] : []),
      ...(name ? [["data-profile-name", ""]] : []),
      ...(handle ? [["data-profile-handle", ""]] : []),
    ]),
    children: [],
    textContent: "",
    getAttribute(key) { return this.attributes.get(key) ?? null; },
    setAttribute(key, value) { this.attributes.set(key, String(value)); },
    hasAttribute(key) { return this.attributes.has(key); },
    matches(selector) {
      const attribute = /^\[([\w-]+)\]$/.exec(selector)?.[1];
      return attribute ? this.attributes.has(attribute) : false;
    },
    querySelectorAll(selector) {
      const attribute = /^\[([\w-]+)\]$/.exec(selector)?.[1];
      const found = [];
      const visit = (node) => {
        for (const child of node.children) {
          if (attribute && child.hasAttribute(attribute)) found.push(child);
          visit(child);
        }
      };
      visit(this);
      return found;
    },
    append(...nodes) { this.children.push(...nodes); },
    replaceWith(node) { this.replacedWith = node; },
  });
  const authorCard = (did) => {
    const card = makeElement({ did });
    card.append(makeElement({ name: true }), makeElement({ handle: true }));
    return card;
  };
  return { makeElement, authorCard };
}

Deno.test("profile fill batches marked DIDs into one request and replaces every element for a DID", async () => {
  const source = await Deno.readTextFile(new URL("../static/app.js", import.meta.url));
  const start = source.indexOf("function fillProfileIdentities(");
  const end = source.indexOf("function installInfiniteScroll", start);
  assert(source.includes("\nfillProfileIdentities();"), "app.js should fill profile placeholders after the page is visible");
  assert(start >= 0 && end > start, "app.js should install background profile fills");
  const fillProfileIdentities = new Function(`${source.slice(start, end)}; return fillProfileIdentities;`)();
  const { makeElement, authorCard } = createProfileFillHarness();

  const sharedFirst = authorCard("did:plc:sharedauthor");
  const sharedSecond = authorCard("did:plc:sharedauthor");
  const sharedAvatar = makeElement({ did: "did:plc:sharedauthor", avatar: true });
  const other = authorCard("did:plc:otherauthor");
  const otherAvatar = makeElement({ did: "did:plc:otherauthor", avatar: true });
  const marked = [sharedFirst, sharedAvatar, sharedSecond, other, otherAvatar];
  const root = {
    matches() { return false; },
    querySelectorAll(selector) { return selector === "[data-profile-did]" ? marked : []; },
  };

  const requests = [];
  const originalFetch = globalThis.fetch;
  const originalDocument = globalThis.document;
  globalThis.document = { createElement: (tag) => makeElement({ tag }) };
  globalThis.fetch = async (url, options) => {
    requests.push({ url, options });
    return {
      ok: true,
      json: async () => ({
        profiles: {
          "did:plc:sharedauthor": { handle: "shared.example", displayName: "Shared Author", avatar: "https://cdn.bsky.app/img/avatar_thumbnail/plain/did:plc:sharedauthor/bafk@jpeg" },
          "did:plc:otherauthor": { handle: "other.example", displayName: "Other Author", avatar: "https://cdn.bsky.app/img/avatar_thumbnail/plain/did:plc:otherauthor/bafk@jpeg" },
        },
      }),
    };
  };
  try {
    fillProfileIdentities(root);
    await new Promise((resolve) => setTimeout(resolve, 0));
    assertEquals(requests.length, 1, "duplicate DIDs across cards must collapse into one batched request");
    assert(requests[0].url.startsWith("/profile-fragment?"), requests[0].url);
    assert(requests[0].url.includes("did=did%3Aplc%3Asharedauthor"), requests[0].url);
    assert(requests[0].url.includes("did=did%3Aplc%3Aotherauthor"), requests[0].url);
    assertEquals(requests[0].options.credentials, "same-origin");
    assertEquals(requests[0].options.headers.Accept, "application/json");
    for (const card of [sharedFirst, sharedSecond]) {
      assertEquals(card.children[0].textContent, "Shared Author");
      assertEquals(card.children[1].textContent, "@shared.example");
    }
    assertEquals(other.children[0].textContent, "Other Author");
    assertEquals(other.children[1].textContent, "@other.example");
    for (const avatar of [sharedAvatar, otherAvatar]) {
      assert(avatar.replacedWith, "the placeholder avatar should be replaced");
      assertEquals(avatar.replacedWith.tagName, "IMG");
      assert(avatar.replacedWith.getAttribute("src").startsWith("https://cdn.bsky.app/img/"), avatar.replacedWith.getAttribute("src"));
      assert(!avatar.replacedWith.className.includes("avatar-placeholder"), avatar.replacedWith.className);
    }
  } finally {
    globalThis.fetch = originalFetch;
    globalThis.document = originalDocument;
  }
});

Deno.test("profile fill sends at most 25 DIDs in one request", async () => {
  const source = await Deno.readTextFile(new URL("../static/app.js", import.meta.url));
  const start = source.indexOf("function fillProfileIdentities(");
  const end = source.indexOf("function installInfiniteScroll", start);
  assert(source.includes("\nfillProfileIdentities();"), "app.js should fill profile placeholders after the page is visible");
  const fillProfileIdentities = new Function(`${source.slice(start, end)}; return fillProfileIdentities;`)();
  const { authorCard } = createProfileFillHarness();
  const marked = Array.from({ length: 30 }, (_, index) => authorCard(`did:plc:batchauthor${index}`));
  const root = {
    matches() { return false; },
    querySelectorAll(selector) { return selector === "[data-profile-did]" ? marked : []; },
  };
  const requests = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, options) => {
    requests.push({ url, options });
    return { ok: true, json: async () => ({ profiles: {} }) };
  };
  try {
    fillProfileIdentities(root);
    await new Promise((resolve) => setTimeout(resolve, 0));
    assertEquals(requests.length, 2, "30 DIDs must split into two public-batch-sized requests");
    assertEquals(requests[0].url.split("did=").length - 1, 25);
    assertEquals(requests[1].url.split("did=").length - 1, 5);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

Deno.test("profile fill runs on cards appended by infinite scroll", async () => {
  const pages = {};
  const harness = await createInfiniteScrollHarness({
    responses: [{ ok: true, text: async () => "page-two" }],
    pages,
  });
  const card = new harness.Element("li", "post-late");
  card.setAttribute("data-post-kind", "post");
  const author = new harness.Element("div");
  author.setAttribute("data-profile-did", "did:plc:lateauthor");
  const name = new harness.Element("a");
  name.setAttribute("data-profile-name", "");
  const handle = new harness.Element("a");
  handle.setAttribute("data-profile-handle", "");
  author.append(name, handle);
  card.append(author);
  const nextList = new harness.Element("ol");
  nextList.className = "post-list";
  nextList.append(card);
  pages["page-two"] = { list: nextList, link: null };

  const requests = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, options) => {
    requests.push({ url, options });
    return {
      ok: true,
      json: async () => ({
        profiles: { "did:plc:lateauthor": { handle: "late.example", displayName: "Late Author" } },
      }),
    };
  };
  try {
    harness.Observer.latest.target.top = -1;
    harness.scroll();
    await new Promise((resolve) => setTimeout(resolve, 0));
    assert(
      requests.some((request) => request.url.startsWith("/profile-fragment?") && request.url.includes("did=did%3Aplc%3Alateauthor")),
      `appended cards should trigger the profile fill: ${JSON.stringify(requests.map((request) => request.url))}`,
    );
    await new Promise((resolve) => setTimeout(resolve, 0));
    assertEquals(name.textContent, "Late Author");
    assertEquals(handle.textContent, "@late.example");
  } finally {
    globalThis.fetch = originalFetch;
  }
});

Deno.test("bar debug overlay shows bottom-bar metrics only when ?bar-debug=1", async () => {
  const source = await Deno.readTextFile(new URL("../static/app.js", import.meta.url));
  const css = await Deno.readTextFile(new URL("../static/app.css", import.meta.url));
  const start = source.indexOf("function installBarDebugOverlay");
  assert(start >= 0, "app.js must define installBarDebugOverlay for ?bar-debug=1 pages");
  const call = source.indexOf("installBarDebugOverlay();", start);
  assert(call > start, "app.js must run the bar debug guard on load");
  const overlay = new Function(`${source.slice(start, call)}; return installBarDebugOverlay;`)();

  assert(/\.bar-debug-probe\s*\{[^}]*padding-top:\s*env\(safe-area-inset-top/.test(css));
  assert(/\.bar-debug-probe\s*\{[^}]*padding-bottom:\s*env\(safe-area-inset-bottom/.test(css));
  assert(/\.bar-debug-box\s*\{[^}]*position:\s*fixed/.test(css), "the debug box must be fixed to the viewport");
  assert(/\.bar-debug-box\s*\{[^}]*top:/.test(css) && !/\.bar-debug-box\s*\{[^}]*bottom:/.test(css),
    "the debug box must sit at the top and never cover the bottom bar");
  assert(/\.bar-debug-box\s*\{[^}]*z-index:\s*(\d{3,}|[6-9]\d)/.test(css), "the debug box needs a high z-index");

  const makeHarness = (search) => {
    const bodyChildren = [];
    const windowListeners = new Map();
    const documentMock = {
      createElement() {
        return {
          className: "",
          textContent: "",
          attributes: new Map(),
          setAttribute(name, value) { this.attributes.set(name, String(value)); },
        };
      },
      body: { append: (...nodes) => bodyChildren.push(...nodes) },
      documentElement: { clientHeight: 844 },
      querySelector() {
        return {
          getBoundingClientRect: () => ({ top: 792, bottom: 844, height: 52 }),
          querySelector: (selector) =>
            selector === ".icon" ? { getBoundingClientRect: () => ({ top: 800, bottom: 840, height: 40 }) } : null,
        };
      },
    };
    const windowMock = {
      location: { search },
      innerHeight: 844,
      devicePixelRatio: 3,
      screen: { height: 926 },
      visualViewport: { height: 802, offsetTop: 42, addEventListener() {} },
      matchMedia() { return { matches: false }; },
      getComputedStyle() { return { paddingTop: "47px", paddingBottom: "34px" }; },
      addEventListener(name, listener) { windowListeners.set(name, listener); },
    };
    return { bodyChildren, documentMock, windowListeners, windowMock };
  };

  const withParam = makeHarness("?bar-debug=1");
  overlay(withParam.windowMock, withParam.documentMock);
  const box = withParam.bodyChildren.find((node) => node.className === "bar-debug-box");
  assert(box, "?bar-debug=1 must add a fixed debug box");
  assert(withParam.bodyChildren.some((node) => node.className === "bar-debug-probe"),
    "?bar-debug=1 must add the safe-area probe element");
  const lines = Object.fromEntries(
    box.textContent.split("\n").map((line) => {
      const at = line.indexOf(":");
      return [line.slice(0, at).trim(), line.slice(at + 1).trim()];
    }),
  );
  assertEquals(lines["innerHeight"], "844");
  assertEquals(lines["vv.height"], "802");
  assertEquals(lines["vv.offsetTop"], "42");
  assertEquals(lines["clientHeight"], "844");
  assertEquals(lines["screen.height"], "926");
  assertEquals(lines["devicePixelRatio"], "3");
  assertEquals(lines["standalone"], "no");
  assertEquals(lines["safe-area-inset-top"], "47px");
  assertEquals(lines["safe-area-inset-bottom"], "34px");
  assertEquals(lines["bar.top"], "792");
  assertEquals(lines["bar.bottom"], "844");
  assertEquals(lines["bar.height"], "52");
  assertEquals(lines["icon.top"], "800");
  assertEquals(lines["icon.bottom"], "840");
  assertEquals(lines["gap bar.top->icon.top"], "8");
  assertEquals(lines["gap icon.bottom->bar.bottom"], "4");
  assertEquals(lines["gap bar.bottom->innerHeight"], "0");
  assert(withParam.windowListeners.has("resize") && withParam.windowListeners.has("scroll"),
    "the debug box must refresh on resize and scroll");
  withParam.windowMock.innerHeight = 700;
  withParam.windowListeners.get("resize")();
  assert(box.textContent.includes("innerHeight: 700"), "the box must refresh when the viewport changes");

  const withoutParam = makeHarness("?ref=share");
  overlay(withoutParam.windowMock, withoutParam.documentMock);
  assertEquals(withoutParam.bodyChildren.length, 0);
  assertEquals(withoutParam.windowListeners.size, 0);
});

async function createKeyboardClearanceHarness({ textareaBottom = 700, withVisualViewport = true } = {}) {
  const documentListeners = new Map();
  const viewportListeners = new Map();
  const scroller = {
    className: "app-layout",
    scrollTop: 0,
    scrollHeight: 3000,
    clientHeight: 600,
    parentElement: null,
  };
  const textarea = {
    tagName: "TEXTAREA",
    parentElement: scroller,
    getBoundingClientRect: () => ({ bottom: textareaBottom - scroller.scrollTop }),
  };
  const visualViewport = {
    height: 500,
    offsetTop: 0,
    addEventListener(name, listener) { viewportListeners.set(name, listener); },
  };
  const windowMock = {
    scrollY: 0,
    scrollX: 0,
    scrollTo(x, y) { this.scrollX = x; this.scrollY = y; },
    getComputedStyle(element) { return { overflowY: element === scroller ? "auto" : "visible" }; },
    matchMedia() { return { matches: true }; },
    addEventListener() {},
  };
  if (withVisualViewport) windowMock.visualViewport = visualViewport;
  const documentMock = {
    activeElement: null,
    addEventListener(name, listener) { documentListeners.set(name, listener); },
    querySelector(selector) { return selector === ".mobile-nav" ? {} : null; },
    querySelectorAll() { return []; },
  };
  globalThis.document = documentMock;
  globalThis.window = windowMock;
  globalThis.HTMLFormElement = class {};
  globalThis.Node = { TEXT_NODE: 3 };
  const source = await Deno.readTextFile(new URL("../static/app.js", import.meta.url));
  new Function(source)();
  const resets = [];
  let phase = "load";
  windowMock.scrollTo = (x, y) => { windowMock.scrollX = x; windowMock.scrollY = y; resets.push(phase); };
  return { documentMock, documentListeners, phase: () => phase, resets, scroller, setPhase: (name) => { phase = name; }, textarea, viewportListeners, visualViewport, windowMock };
}

Deno.test("focused composer textarea scrolls its inner scroller by exactly the keyboard overlap", async () => {
  const harness = await createKeyboardClearanceHarness({ textareaBottom: 700 });
  harness.windowMock.scrollY = 200;
  harness.documentMock.activeElement = harness.textarea;
  harness.documentListeners.get("focusin")();
  assertEquals(harness.scroller.scrollTop, 256, "700 - (0 + 500 - 56) = 256 px of overlap must move the inner scroller");
  harness.viewportListeners.get("resize")();
  assertEquals(harness.scroller.scrollTop, 256, "once the box clears the 56 px area a keyboard resize must not scroll again");
  assertEquals(harness.resets.length, 0, "a visualViewport resize while a text field is focused must not call window.scrollTo(0, 0)");
});

Deno.test("a composer textarea that already clears the keyboard area is left alone", async () => {
  const harness = await createKeyboardClearanceHarness({ textareaBottom: 300 });
  harness.windowMock.scrollY = 140;
  harness.documentMock.activeElement = harness.textarea;
  harness.documentListeners.get("focusin")();
  harness.setPhase("resize");
  harness.viewportListeners.get("resize")();
  assertEquals(harness.scroller.scrollTop, 0, "300 is above 500 - 56, so nothing may scroll");
  assertEquals(harness.resets.length, 0, "the keyboard resize while focused must not reset the document scroll");
});

Deno.test("missing visualViewport skips the keyboard clearance without error or scroll", async () => {
  const harness = await createKeyboardClearanceHarness({ textareaBottom: 700, withVisualViewport: false });
  harness.documentMock.activeElement = harness.textarea;
  harness.documentListeners.get("focusin")();
  assertEquals(harness.scroller.scrollTop, 0, "desktop and old browsers have no visualViewport, so the page must not move");
});

Deno.test("focusout still resets a document the closing keyboard left scrolled", async () => {
  const harness = await createKeyboardClearanceHarness({ textareaBottom: 700 });
  harness.windowMock.scrollY = 120;
  harness.documentMock.activeElement = harness.textarea;
  harness.setPhase("focus");
  harness.documentListeners.get("focusin")();
  harness.setPhase("resize");
  harness.viewportListeners.get("resize")();
  assertEquals(harness.resets.length, 0, "the keyboard opening must not reset the document while the textarea is focused");
  harness.setPhase("focusout");
  harness.documentMock.activeElement = null;
  harness.documentListeners.get("focusout")();
  await new Promise((resolve) => setTimeout(resolve, 150));
  assertEquals(harness.resets.join(","), "focusout", "focusout keeps the original document reset (its 100 ms timeout included)");
});
