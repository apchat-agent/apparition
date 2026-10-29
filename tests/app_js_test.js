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
