let textColor = document.getElementById("textColor");
let bgColor = document.getElementById("bgColor");
let result = document.querySelector(".result");
let contrastRef = document.getElementById("rating");

//Function to convert hex value to RGB array
function hexToRGB(colorValue) {
const red = parseInt(colorValue.substring(1, 3), 16);
const green = parseInt(colorValue.substring(3, 5), 16);
const blue = parseInt(colorValue.substring(5, 7), 16);
return [red, green, blue];
}

let getRelativeLuminance = (color) => {
const sRGB = color.map((val) => {
    const s = val / 255;
    return s < 0.03928 ? s / 12 / 92 : Math.pow((s + 0.055) / 1.055, 2.4);
});
return 0.2126 * sRGB[0] + 0.7152 * sRGB[1] + 0.0722 * sRGB[2];
};

let calculateContrastRatio = (color1, color2) => {
const luminance1 = getRelativeLuminance(color1);
const luminance2 = getRelativeLuminance(color2);

const light = Math.max(luminance1, luminance2);
const dark = Math.min(luminance1, luminance2);
const contrast = (light + 0.05) / (dark + 0.05);
return contrast;
};

let calcRating = (contrastVal) => {
    if (contrastVal > 12) {
        result.style.backgroundColor = "#69eb67";
        return "Super";
    } else if (contrastVal > 7) {
        result.style.backgroundColor = "#b7ea84";
        return "Very Good";
    } else if (contrastVal > 5) {
        result.style.backgroundColor = "#f7d658";
        return "Good";
    } else if (contrastVal > 3) {
        result.style.backgroundColor = "#f17a55";
        return "Poor";
    } else {
        result.style.backgroundColor = "#f24646";
        return "Very Poor";
    }
};

let contrastChecker = (button) => {
let textColorValue = textColor.value;
let textColorRGBArray = hexToRGB(textColorValue);

let bgColorValue = bgColor.value;
let bgColorRGBArray = hexToRGB(bgColorValue);

const contrast = calculateContrastRatio(textColorRGBArray, bgColorRGBArray);

contrastRef.innerText = contrast.toFixed(2);
contrastRef.innerText = calcRating(contrast);
result.style.color = textColorValue;
result.style.backgroundColor = bgColorValue;
};

textColor.addEventListener("input", contrastChecker);
bgColor.addEventListener("input", contrastChecker);
window.addEventListener("load", contrastChecker);