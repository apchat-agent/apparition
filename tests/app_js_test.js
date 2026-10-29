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
    createElement() {
      return { className: "", textContent: "" };
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
