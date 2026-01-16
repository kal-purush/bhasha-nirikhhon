// Get Canvas and Context
const canvas = document.getElementById("MyCanvas");
const ctx = canvas.getContext("2d");

// Set initial canvas size and update on resize
canvas.height = window.innerHeight;
canvas.width = window.innerWidth;
window.addEventListener("resize", () => {
  canvas.height = window.innerHeight;
  canvas.width = window.innerWidth;
});

// Default Wave Properties
const defaultWave = {
  Frequency: 0.18008,
  Amplitude: 80,
  amplitudeSwing: 0, // 0 (off) or 1 (on)
  SwingNumber: 100,
  frequencySwing: 0, // 0 (off) or 1 (on)
  fswingno: 1e9,
  modulation: 0, // 0 (off) or 1 (on)
  ModulationCoeffiecient: 0.050,
  modXMap: 0.819,
  bellMode: 0, // 0 (off) or 1 (on)
  peakBellCurve: 3.5,
  bellXMap: 0.008,
  dampXMap: 0.009,
  Damping: 0,
  axis: 0,
  offsetX: 0,
  offsetY: 0,
};

// Wave Properties Object (initially set to defaultWave)
const Wave = { ...defaultWave };

// ------------------
// Control Panel Setup
// ------------------

// Panel toggle button
const togglePanelBtn = document.getElementById("togglePanelBtn");
const inputContainer = document.getElementById("inputContainer");
togglePanelBtn.addEventListener("click", () => {
  if (inputContainer.classList.contains("inputContainerOFF")) {
    inputContainer.classList.remove("inputContainerOFF");
    inputContainer.classList.add("inputContainer");
    togglePanelBtn.innerText = "Hide Panel";
  } else {
    inputContainer.classList.remove("inputContainer");
    inputContainer.classList.add("inputContainerOFF");
    togglePanelBtn.innerText = "Show Panel";
  }
});

// --- Utility Function to Update a Slider's Display ---
function updateSlider(slider, display) {
  display.innerText = slider.value;
  return parseFloat(slider.value);
}

// Amplitude Slider
const amplitudeSlider = document.getElementById("amplitudeSlider");
const amplitudeValue = document.getElementById("amplitudeValue");
amplitudeValue.innerText = amplitudeSlider.value;
amplitudeSlider.oninput = () => {
  Wave.Amplitude = updateSlider(amplitudeSlider, amplitudeValue);
};

// Frequency Slider
const frequencySlider = document.getElementById("frequencySlider");
const frequencyValue = document.getElementById("frequencyValue");
frequencyValue.innerText = frequencySlider.value;
frequencySlider.oninput = () => {
  Wave.Frequency = updateSlider(frequencySlider, frequencyValue);
};

// Amplitude Swing Toggle
const amplitudeSwingSwitch = document.getElementById("amplitudeSwingSwitch");
amplitudeSwingSwitch.onchange = () => {
  Wave.amplitudeSwing = amplitudeSwingSwitch.checked ? 1 : 0;
};

// Swing Number Slider
const swingNumberSlider = document.getElementById("swingNumberSlider");
const swingNumberValue = document.getElementById("swingNumberValue");
swingNumberValue.innerText = swingNumberSlider.value;
swingNumberSlider.oninput = () => {
  Wave.SwingNumber = updateSlider(swingNumberSlider, swingNumberValue);
};

// Frequency Swing Toggle
const frequencySwingSwitch = document.getElementById("frequencySwingSwitch");
frequencySwingSwitch.onchange = () => {
  Wave.frequencySwing = frequencySwingSwitch.checked ? 1 : 0;
};

// Frequency Swing Number Slider
const frequencySwingNumberSlider = document.getElementById("frequencySwingNumberSlider");
const frequencySwingNumberValue = document.getElementById("frequencySwingNumberValue");
frequencySwingNumberValue.innerText = frequencySwingNumberSlider.value;
frequencySwingNumberSlider.oninput = () => {
  Wave.fswingno = updateSlider(frequencySwingNumberSlider, frequencySwingNumberValue);
};

// Modulation Toggle
const modulationSwitch = document.getElementById("modulationSwitch");
modulationSwitch.onchange = () => {
  Wave.modulation = modulationSwitch.checked ? 1 : 0;
};

// Modulation Coefficient Slider
const modulationCoefficientSlider = document.getElementById("modulationCoefficientSlider");
const modulationCoefficientValue = document.getElementById("modulationCoefficientValue");
modulationCoefficientValue.innerText = modulationCoefficientSlider.value;
modulationCoefficientSlider.oninput = () => {
  Wave.ModulationCoeffiecient = updateSlider(modulationCoefficientSlider, modulationCoefficientValue);
};

// modXMap Slider
const modXMapSlider = document.getElementById("modXMapSlider");
const modXMapValue = document.getElementById("modXMapValue");
modXMapValue.innerText = modXMapSlider.value;
modXMapSlider.oninput = () => {
  Wave.modXMap = updateSlider(modXMapSlider, modXMapValue);
};

// Bell Mode Toggle
const bellModeSwitch = document.getElementById("bellModeSwitch");
bellModeSwitch.onchange = () => {
  Wave.bellMode = bellModeSwitch.checked ? 1 : 0;
};

// Peak Bell Curve Slider
const peakBellCurveSlider = document.getElementById("peakBellCurveSlider");
const peakBellCurveValue = document.getElementById("peakBellCurveValue");
peakBellCurveValue.innerText = peakBellCurveSlider.value;
peakBellCurveSlider.oninput = () => {
  Wave.peakBellCurve = updateSlider(peakBellCurveSlider, peakBellCurveValue);
};

// bellXMap Slider
const bellXMapSlider = document.getElementById("bellXMapSlider");
const bellXMapValue = document.getElementById("bellXMapValue");
bellXMapValue.innerText = bellXMapSlider.value;
bellXMapSlider.oninput = () => {
  Wave.bellXMap = updateSlider(bellXMapSlider, bellXMapValue);
};

// Damped X Map Slider
const dampXMapSlider = document.getElementById("dampXMapSlider");
const dampXMapValue = document.getElementById("dampXMapValue");
dampXMapValue.innerText = dampXMapSlider.value;
dampXMapSlider.oninput = () => {
  Wave.dampXMap = updateSlider(dampXMapSlider, dampXMapValue);
};

// Damping Slider
const dampingSlider = document.getElementById("dampingSlider");
const dampingValue = document.getElementById("dampingValue");
dampingValue.innerText = dampingSlider.value;
dampingSlider.oninput = () => {
  Wave.Damping = updateSlider(dampingSlider, dampingValue);
};

// Axis Slider
const axisSlider = document.getElementById("axisSlider");
const axisValue = document.getElementById("axisValue");
axisValue.innerText = axisSlider.value;
axisSlider.oninput = () => {
  Wave.axis = updateSlider(axisSlider, axisValue);
};

// Offset X Slider
const offsetXSlider = document.getElementById("offsetXSlider");
const offsetXValue = document.getElementById("offsetXValue");
offsetXValue.innerText = offsetXSlider.value;
offsetXSlider.oninput = () => {
  Wave.offsetX = updateSlider(offsetXSlider, offsetXValue);
};

// Offset Y Slider
const offsetYSlider = document.getElementById("offsetYSlider");
const offsetYValue = document.getElementById("offsetYValue");
offsetYValue.innerText = offsetYSlider.value;
offsetYSlider.oninput = () => {
  Wave.offsetY = updateSlider(offsetYSlider, offsetYValue);
};

// ------------------
// Reset Button Setup
// ------------------
const resetBtn = document.getElementById("resetBtn");
resetBtn.addEventListener("click", () => {
  // Reset Wave properties
  Object.assign(Wave, defaultWave);

  // Update all slider and toggle controls
  amplitudeSlider.value = defaultWave.Amplitude;
  amplitudeValue.innerText = defaultWave.Amplitude;

  frequencySlider.value = defaultWave.Frequency;
  frequencyValue.innerText = defaultWave.Frequency;

  amplitudeSwingSwitch.checked = defaultWave.amplitudeSwing === 1;
  swingNumberSlider.value = defaultWave.SwingNumber;
  swingNumberValue.innerText = defaultWave.SwingNumber;

  frequencySwingSwitch.checked = defaultWave.frequencySwing === 1;
  frequencySwingNumberSlider.value = defaultWave.fswingno;
  frequencySwingNumberValue.innerText = defaultWave.fswingno;

  modulationSwitch.checked = defaultWave.modulation === 1;
  modulationCoefficientSlider.value = defaultWave.ModulationCoeffiecient;
  modulationCoefficientValue.innerText = defaultWave.ModulationCoeffiecient;

  modXMapSlider.value = defaultWave.modXMap;
  modXMapValue.innerText = defaultWave.modXMap;

  bellModeSwitch.checked = defaultWave.bellMode === 1;
  peakBellCurveSlider.value = defaultWave.peakBellCurve;
  peakBellCurveValue.innerText = defaultWave.peakBellCurve;

  bellXMapSlider.value = defaultWave.bellXMap;
  bellXMapValue.innerText = defaultWave.bellXMap;

  dampXMapSlider.value = defaultWave.dampXMap;
  dampXMapValue.innerText = defaultWave.dampXMap;

  dampingSlider.value = defaultWave.Damping;
  dampingValue.innerText = defaultWave.Damping;

  axisSlider.value = defaultWave.axis;
  axisValue.innerText = defaultWave.axis;

  offsetXSlider.value = defaultWave.offsetX;
  offsetXValue.innerText = defaultWave.offsetX;

  offsetYSlider.value = defaultWave.offsetY;
  offsetYValue.innerText = defaultWave.offsetY;
});

// ------------------
// Wave Rendering Code
// ------------------

class wavePixels {
  constructor(context, x, y, dampXMap, modXMap, bellXMap) {
    this.ctx = context;
    this.initialPos = { x: x, y: y };
    this.pos = { x: this.initialPos.x, y: this.initialPos.y };
    this.dampXMap = dampXMap;
    this.modXMap = modXMap;
    this.bellXMap = bellXMap;
    this.r = 0;
    this.g = 0;
    this.b = 0;
  }
  draw() {
    this.ctx.save();
    this.ctx.translate(Wave.offsetX, canvas.height / 2 + Wave.offsetY);
    this.ctx.rotate(Wave.axis);
    this.ctx.fillStyle = `rgba(${this.r}, ${this.g}, ${this.b}, 1)`;
    this.ctx.beginPath();
    this.ctx.arc(this.initialPos.x, this.pos.y, 2, 0, Math.PI * 2);
    this.ctx.fill();
    this.ctx.restore();
  }
  update() {
    this.pos.x += 0.8;
    this.pos.y =
      Math.exp(-this.dampXMap * Wave.Damping) *
      Math.pow(Math.sin(this.modXMap * Wave.ModulationCoeffiecient), Wave.modulation) *
      Math.exp(-Math.pow(this.bellXMap - Wave.peakBellCurve, 2) * Wave.bellMode) *
      Wave.Amplitude *
      Math.pow(Math.sin(time), Wave.amplitudeSwing) *
      Math.sin(Wave.Frequency * Math.pow(Math.sin(ftime), Wave.frequencySwing) * this.pos.x);
      
    // Increment color (wraps around 256)
    this.r = (this.r + 150) % 256;
  }
}

class ControlPanel {
  constructor(context) {
    this.context = context;
    this.x = 0;
    this.y = 0;
    this.dampXMap = 0;
    this.modXMap = 0;
    this.bellXmap = 0;
    this.gap = 1;
    this.pixelArray = [];
  }
  generate() {
    this.pixelArray.push(new wavePixels(this.context, this.x, this.y, this.dampXMap, this.modXMap, this.bellXmap));
  }
  render() {
    this.pixelArray.forEach((e) => {
      e.draw();
      e.update();
    });
  }
  destroyPixel() {
    if (this.pixelArray.length >= 3000) {
      for (let i = 0; i < 2; i++) this.pixelArray.pop();
    }
  }
  update() {
    this.x += 0.51 + this.gap;
    this.dampXMap += Wave.dampXMap;
    this.modXMap += Wave.modXMap;
    this.bellXmap += Wave.bellXMap;
  }
}

const control = new ControlPanel(ctx);
ctx.fillStyle = "rgb(0, 0, 0)";
let time = 0;
let ftime = 0;

function animate() {
  // Create a trailing effect using a semi-transparent fill
  ctx.fillRect(0, 0, canvas.width, canvas.height);

  control.generate();
  control.render();
  control.update();
  control.destroyPixel();

  time += 1 / Wave.SwingNumber;
  ftime += 1 / Wave.fswingno;

  requestAnimationFrame(animate);
}
animate();