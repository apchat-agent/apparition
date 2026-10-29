// @ts-nocheck

function assert(value, message = "assertion failed") {
  if (!value) throw new Error(message);
}

function assertEquals(actual, expected) {
  assert(actual === expected, `expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
}

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

Deno.test("alt suggestion loads only after its button is clicked and protects edits", async () => {
  const source = await Deno.readTextFile(new URL("../static/app.js", import.meta.url));
  globalThis.document = {
    addEventListener() {},
    querySelector() { return null; },
    querySelectorAll() { return []; },
  };
  globalThis.window = { visualViewport: { addEventListener() {} } };
  const attachAltSuggestion = new Function(`${source}; return attachAltSuggestion;`)();
  assertEquals(typeof attachAltSuggestion, "function");

  let clickHandler;
  let modelLoads = 0;
  const button = {
    disabled: false,
    addEventListener(name, listener) {
      assertEquals(name, "click");
      clickHandler = listener;
    },
    click() {
      return clickHandler();
    },
  };
  const item = { file: { name: "local.jpg" }, alt: "", suggestedAlt: "" };
  const altInput = { value: "" };
  const status = { textContent: "" };
  const progress = { hidden: true, max: 0, value: 0 };
  const suggestions = ["A dog on a beach.", "A brown dog on a beach.", "A dog beside the sea."];
  const loader = async () => {
    modelLoads++;
    assert(!progress.hidden, "the first click should reveal download progress");
    assertEquals(progress.max, 264_713_119, "progress should show the exact model payload size");
    assert(status.textContent.includes("264,713,119 bytes"), "the download size should be visible before loading");
    return { caption: async () => suggestions.shift() };
  };

  attachAltSuggestion(button, item, altInput, status, progress, loader);
  assertEquals(modelLoads, 0, "the model must stay unloaded until the user taps the button");
  await button.click();
  assertEquals(modelLoads, 1);
  assertEquals(altInput.value, "A dog on a beach.");
  assertEquals(item.alt, "A dog on a beach.");

  altInput.value = "My own description.";
  await button.click();
  assertEquals(altInput.value, "My own description.", "a user edit must win over a later suggestion");
  altInput.value = "A dog on a beach.";
  await button.click();
  assertEquals(altInput.value, "A dog beside the sea.", "the previous suggestion may be replaced");
});

Deno.test("alt suggestion failure leaves the alt text alone and reports a short message", async () => {
  const source = await Deno.readTextFile(new URL("../static/app.js", import.meta.url));
  globalThis.document = {
    addEventListener() {},
    querySelector() { return null; },
    querySelectorAll() { return []; },
  };
  globalThis.window = { visualViewport: { addEventListener() {} } };
  const attachAltSuggestion = new Function(`${source}; return attachAltSuggestion;`)();
  assertEquals(typeof attachAltSuggestion, "function");
  let clickHandler;
  const button = {
    disabled: false,
    addEventListener(_name, listener) {
      clickHandler = listener;
    },
    click() {
      return clickHandler();
    },
  };
  const item = { file: { name: "local.jpg" }, alt: "Carefully written alt text", suggestedAlt: "" };
  const altInput = { value: item.alt };
  const status = { textContent: "" };
  const progress = { hidden: true, max: 0, value: 0 };

  attachAltSuggestion(button, item, altInput, status, progress, async () => {
    throw new Error("fake model download failure");
  });
  await button.click();
  assertEquals(altInput.value, "Carefully written alt text");
  assert(status.textContent.length > 0, "a useful failure message should be visible");
  assert(!button.disabled, "the button should be usable after a failure");
});

Deno.test("each selected image renders an opt-in alt suggestion button", async () => {
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
      if (selector === "[data-image-input]") return input;
      if (selector === "[data-image-previews]") return previews;
      if (selector === "[data-image-status]") return status;
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
    querySelector(selector) { return selector === "form[data-alt-test]" ? null : null; },
    querySelectorAll(selector) { return selector === "form[data-image-composer]" ? [form] : []; },
  };
  globalThis.window = { visualViewport: { addEventListener() {} } };
  new Function(source)();
  input.listeners.get("change")();

  const preview = previews.children[0];
  assert(preview, "choosing a picture should create its preview");
  assert(!linkImageNote.hidden, "pictures and a link should show the precedence notice");
  const details = preview.children[1];
  const suggestion = details.children.find((child) => child.tag === "button" && child.textContent === "Suggest alt text");
  assert(suggestion, "the alt field should have its own suggestion button");
  assertEquals(suggestion.type, "button", "suggesting must not submit the post form");
  assertEquals(typeof suggestion.listeners.get("click"), "function");
  const remove = details.children.find((child) => child.tag === "button" && child.textContent === "Remove");
  remove.listeners.get("click")();
  assert(linkImageNote.hidden, "removing the last picture should clear the precedence notice");
});

Deno.test("posting completes while an alt suggestion is still running", async () => {
  const source = await Deno.readTextFile(new URL("../static/app.js", import.meta.url));
  globalThis.document = {
    addEventListener() {},
    querySelector() { return null; },
    querySelectorAll() { return []; },
  };
  globalThis.window = { visualViewport: { addEventListener() {} }, location: { assign() {} } };
  const helpers = new Function(`${source}; return { attachAltSuggestion, handleImageComposerSubmit };`)();
  assertEquals(typeof helpers.handleImageComposerSubmit, "function");

  let releaseModel;
  const pendingModel = new Promise((resolve) => { releaseModel = resolve; });
  let suggestionFinished = false;
  let clickHandler;
  const suggestionButton = {
    disabled: false,
    addEventListener(_name, listener) { clickHandler = listener; },
    click() { return clickHandler(); },
  };
  helpers.attachAltSuggestion(
    suggestionButton,
    { file: { name: "local.png" }, alt: "", suggestedAlt: "" },
    { value: "" },
    { textContent: "" },
    { hidden: true, max: 0, value: 0 },
    () => pendingModel,
  );
  const suggestion = suggestionButton.click().then(() => { suggestionFinished = true; });
  await Promise.resolve();

  const submitButton = { disabled: false };
  const error = { hidden: true, textContent: "" };
  const imageInput = { files: [{ name: "local.png" }] };
  const form = {
    action: "/post",
    querySelector(selector) {
      if (selector === 'button[type="submit"]') return submitButton;
      if (selector === "[data-composer-error]") return error;
      if (selector === "[data-image-input]") return imageInput;
      return null;
    },
  };
  const originalFormData = globalThis.FormData;
  const originalFile = globalThis.File;
  const originalFetch = globalThis.fetch;
  let didPost = false;
  class FakeFormData {
    delete() {}
    append() {}
  }
  class FakeFile {
    constructor(parts, name, options) { this.parts = parts; this.name = name; this.type = options.type; }
  }
  globalThis.FormData = FakeFormData;
  globalThis.File = FakeFile;
  globalThis.fetch = async () => {
    didPost = true;
    return { ok: true, url: "/app" };
  };
  try {
    await helpers.handleImageComposerSubmit(
      form,
      [{ file: { name: "local.png" }, alt: "" }],
      false,
      { preventDefault() {} },
      {
        loadImage: async () => ({ width: 1, height: 1 }),
        resizeImage: async () => ({ blob: new Blob(["image"]), width: 1, height: 1 }),
      },
    );
    assert(didPost, "the image post should proceed without waiting for the caption model");
    assert(!suggestionFinished, "the pending caption request must still be pending after posting");
  } finally {
    globalThis.FormData = originalFormData;
    globalThis.File = originalFile;
    globalThis.fetch = originalFetch;
  }
  releaseModel({ caption: async () => "A photo." });
  await suggestion;
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
