{
  window.addEventListener("load", () => {
    document.querySelectorAll('[data-role="accordion-toggle"]').forEach((button) => {
        button.addEventListener("click", () => {
          const faqItem = button.closest(".faq-item");
          const accordion = faqItem.querySelector(".faq-item-description");

          if (faqItem.classList.contains("opened")) {
            accordion.style.height = `${accordion.scrollHeight}px`;
            requestAnimationFrame(() => {
              accordion.style.height = "0";
            });
            faqItem.classList.remove("opened");
          } else {
            accordion.style.height = `${accordion.scrollHeight}px`;
            faqItem.classList.add("opened");

            accordion.addEventListener(
              "transitionend",
              () => {
                if (faqItem.classList.contains("opened")) {
                  accordion.style.height = "auto";
                }
              },
              { once: true }
            );
          }
        });
      });
  });
}