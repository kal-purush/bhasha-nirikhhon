document.addEventListener('DOMContentLoaded', function() {
  const colorPicker = document.getElementById('color-picker');
  const colorDisplay = document.getElementById('color-display');
  const hexColorInput = document.getElementById('hex-color');
  const rgbColorInput = document.getElementById('rgb-color');

  function updateColor() {
    const selectedColor = colorPicker.value;
    colorDisplay.style.backgroundColor = selectedColor;

    const rgbColor = hexToRgb(selectedColor);
    hexColorInput.value = selectedColor;
    rgbColorInput.value = `rgb(${rgbColor.r}, ${rgbColor.g}, ${rgbColor.b})`;
  }

  colorPicker.addEventListener('input', updateColor);

  hexColorInput.addEventListener('input', function() {
    const selectedColor = hexColorInput.value;
    colorDisplay.style.backgroundColor = selectedColor;

    colorPicker.value = selectedColor;
    const rgbColor = hexToRgb(selectedColor);
    rgbColorInput.value = `rgb(${rgbColor.r}, ${rgbColor.g}, ${rgbColor.b})`;
  });

  rgbColorInput.addEventListener('input', function() {
    const selectedColor = rgbColorInput.value;
    colorDisplay.style.backgroundColor = selectedColor;

    colorPicker.value = rgbToHex(selectedColor);
    hexColorInput.value = rgbToHex(selectedColor);
  });

  colorDisplay.addEventListener('click', function() {
    colorPicker.click();
  });

  function hexToRgb(hex) {
    hex = hex.replace('#', '');
    const bigint = parseInt(hex, 16);
    const r = (bigint >> 16) & 255;
    const g = (bigint >> 8) & 255;
    const b = bigint & 255;
    return { r, g, b };
  }

  function rgbToHex(rgb) {
    const rgbaArray = rgb.match(/\d+/g);
    const r = parseInt(rgbaArray[0]);
    const g = parseInt(rgbaArray[1]);
    const b = parseInt(rgbaArray[2]);
    return `#${((1 << 24) + (r << 16) + (g << 8) + b).toString(16).slice(1)}`;
  }

  updateColor();
});