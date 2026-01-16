export const initFaqAccordions = () => {
  if (document.querySelectorAll('[data-faq="accordion-button"]')) {
    const buttons = document.querySelectorAll('[data-faq="accordion-button"]');
    buttons.forEach((button) => {
      button.addEventListener('click', () => {
        const content = button.nextElementSibling;
        const isActive = button.classList.contains('is-active');

        if (isActive) {
          content.style.maxHeight = 0;
        } else {
          content.style.maxHeight = `${content.scrollHeight}px`;
        }

        button.classList.toggle('is-active');
        content.classList.toggle('is-active');
      });
    });
  }
};