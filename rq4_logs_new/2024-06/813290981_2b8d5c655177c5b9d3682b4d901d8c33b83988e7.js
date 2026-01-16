document.addEventListener("DOMContentLoaded", () => {
  const rangeSlider = document.getElementById("rangeSlider");
  const valueDisplay = document.getElementById("valueDisplay");
  const pipsContainer = document.getElementById("pips");

  // Update the value display initially
  valueDisplay.textContent = rangeSlider.value;

  // Create pips
  const min = parseInt(rangeSlider.min, 10);
  const max = parseInt(rangeSlider.max, 10);
  const step = parseInt(rangeSlider.step, 10);

  for (let i = min; i <= max; i += step) {
    const pip = document.createElement("div");
    pip.classList.add("pip");
    pipsContainer.appendChild(pip);
  }

  // Update the value display when the slider value changes
  rangeSlider.addEventListener("input", () => {
    valueDisplay.textContent = rangeSlider.value;
  });
});