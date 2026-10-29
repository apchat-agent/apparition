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

// Phones: the app frame is pinned to the screen (app.css), so the document itself should never be scrolled. iOS can
// still leave it scrolled after the keyboard closes; put it back.
const resetDocumentScroll = () => {
  if (document.querySelector(".mobile-nav") && (window.scrollY !== 0 || window.scrollX !== 0)) window.scrollTo(0, 0);
};
document.addEventListener("focusout", () => setTimeout(resetDocumentScroll, 100));
window.visualViewport?.addEventListener("resize", resetDocumentScroll);
