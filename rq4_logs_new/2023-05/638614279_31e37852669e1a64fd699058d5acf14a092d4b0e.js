const TOGGLE_ALIGNMENT_MODE_BUTTON = document.querySelector(
  '.toggleSolarSystemAlignmentButton'
);
const VERTICAL_ALIGNMENT_CONTAINER = document.querySelector(
  '.solar-system__vertical-alignment'
);
const HORIZONTAL_ALIGNMENT_CONTAINER = document.querySelector(
  '.solar-system__horizontal-alignment'
);

function toggleAlignmentMode() {
  const currentMode = TOGGLE_ALIGNMENT_MODE_BUTTON.dataset.alignmentMode;

  TOGGLE_ALIGNMENT_MODE_BUTTON.dataset.alignmentMode =
    currentMode === 'horizontal' ? 'vertical' : 'horizontal';

  VERTICAL_ALIGNMENT_CONTAINER.classList.toggle('hide');
  HORIZONTAL_ALIGNMENT_CONTAINER.classList.toggle('hide');
}

TOGGLE_ALIGNMENT_MODE_BUTTON.addEventListener('click', toggleAlignmentMode);