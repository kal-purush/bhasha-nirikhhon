import * as $ from "jquery";

let insertionId = 0;

const priorityOptions = [
  {
    displayName: "⛰ Blocking and requires immediate action",
    textInsertion: "⛰ Mountain / **Blocking and requires immediate action**",
  },
  {
    displayName: "🧗‍♀️ Blocking",
    textInsertion: "🧗‍♀️ Boulder / **Blocking**",
  },
  {
    displayName: "⚪️ Non-blocking but requires future action",
    textInsertion: "⚪️ Pebble / Non-blocking but requires future action",
  },
  {
    displayName: "⏳ Non-blocking but requires future consideration",
    textInsertion: "⏳ Sand / Non-blocking but requires future consideration",
  },
  {
    displayName: "🌫 Non-blocking, “take it or leave it”",
    textInsertion: "🌫 Dust / Non-blocking, “take it or leave it”",
  },
];

const observer = new MutationObserver((mutations) => {
  mutations.forEach((mutation) => {
    mutation.addedNodes.forEach((node) => {
      const containerEl = node as Element;
      if (containerEl.className?.includes("js-inline-comments-container")) {
        insertionId += 1;
        const selectId = `priority-${insertionId}`;
        $(".form-actions", node)
          .before(`<select id="${selectId}" style="float: left; height: 34px; margin-left: 8px;">
    ${priorityOptions.map(
      (priority) =>
        `<option value="${priority.textInsertion}">${priority.displayName}</option>`
    )}
  </select>
</div>`);
        $(
          ".form-actions .review-simple-reply-button, .form-actions .btn-primary",
          node
        ).click((e) => {
          const $textarea = $(".comment-form-textarea", node);
          $textarea.val(
            `|Feedback Priority|\n|---|\n|${$(
              `#${selectId}`
            ).val()}|\n\n${$textarea.val()}`
          );
          e.stopPropagation();
        });
      }
    });
  });
});

observer.observe(document.body, { childList: true, subtree: true });