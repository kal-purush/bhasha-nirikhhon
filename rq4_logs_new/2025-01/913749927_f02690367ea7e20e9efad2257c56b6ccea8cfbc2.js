let animationFrameId = null;

function checkData() {
  if (animationFrameId) {
    cancelAnimationFrame(animationFrameId);
  }

  document.getElementById("correctCount").textContent = "0";
  document.getElementById("incorrectCount").textContent = "0";
  document.getElementById("notFoundCount").textContent = "0";
  document.getElementById("validData").innerHTML = "";
  document.getElementById("invalidData").innerHTML = "";
  document.getElementById("notFoundData").innerHTML = "";

  const userWorkData = document.getElementById("userWorkData").value;
  const addressList = document.getElementById("addressList").value;

  const addresses = addressList
    .split("\n")
    .map((addr) => addr.trim())
    .filter((addr) => addr);

  const workDataPairs = userWorkData
    .split("\n")
    .filter((line) => line.trim())
    .reduce((acc, line, index, array) => {
      if (index % 2 === 0 && array[index + 1]) {
        acc.push({
          address: line.trim(),
          uxuy: array[index + 1].trim(),
        });
      }
      return acc;
    }, []);

  let correctCount = 0;
  let incorrectCount = 0;
  let notFoundCount = 0;

  const validData = [];
  const invalidData = [];
  const notFoundData = [];

  addresses.forEach((fullAddress) => {
    const isFound = workDataPairs.some((pair) => {
      const addressPattern = pair.address.replace("...", "");
      const firstFour = addressPattern.substring(0, 4);
      const lastFour = addressPattern.substring(addressPattern.length - 4);
      return (
        fullAddress.startsWith(firstFour) && fullAddress.endsWith(lastFour)
      );
    });

    if (!isFound) {
      notFoundCount++;
      notFoundData.push({
        address: fullAddress,
        uxuy: "Not Found",
      });
    }
  });

  workDataPairs.forEach((pair) => {
    if (!pair.address || !pair.uxuy) return;

    const addressShort = pair.address;
    const uxuyValue = pair.uxuy;

    const addressPattern = addressShort.replace("...", "");
    const firstFour = addressPattern.substring(0, 4);
    const lastFour = addressPattern.substring(addressPattern.length - 4);

    const matchedAddress = addresses.find(
      (addr) => addr.startsWith(firstFour) && addr.endsWith(lastFour)
    );

    if (matchedAddress) {
      if (uxuyValue === "4UXUY") {
        correctCount++;
        validData.push({ address: addressShort, uxuy: uxuyValue });
      }
      if (uxuyValue === "10UXUY") {
        correctCount++;
        validData.push({ address: addressShort, uxuy: uxuyValue });
      }
      if (uxuyValue === "20UXUY") {
        correctCount++;
        validData.push({ address: addressShort, uxuy: uxuyValue });
      }
      if (uxuyValue === "40UXUY") {
        correctCount++;
        validData.push({ address: addressShort, uxuy: uxuyValue });
      }
      if (uxuyValue === "60UXUY") {
        correctCount++;
        validData.push({ address: addressShort, uxuy: uxuyValue });
      }
      if (uxuyValue === "80UXUY") {
        correctCount++;
        validData.push({ address: addressShort, uxuy: uxuyValue });
      }
      if (uxuyValue === "100UXUY") {
        correctCount++;
        validData.push({ address: addressShort, uxuy: uxuyValue });
      } else if (uxuyValue === "0UXUY") {
        incorrectCount++;
        invalidData.push({ address: addressShort, uxuy: uxuyValue });
      }
    }
  });

  animateCount("correctCount", correctCount);
  animateCount("incorrectCount", incorrectCount);
  animateCount("notFoundCount", notFoundCount);

  displaySheet("validData", validData);
  displaySheet("invalidData", invalidData);
  displaySheet("notFoundData", notFoundData);
}

function animateCount(elementId, finalValue) {
  const element = document.getElementById(elementId);
  const duration = 1000;
  const start = 0;
  const increment = (finalValue - start) / (duration / 16);
  let current = start;

  const animate = () => {
    current += increment;
    if (
      (increment > 0 && current >= finalValue) ||
      (increment < 0 && current <= finalValue)
    ) {
      element.textContent = finalValue;
    } else {
      element.textContent = Math.round(current);
      animationFrameId = requestAnimationFrame(animate);
    }
  };

  animate();
}

function displaySheet(elementId, data) {
  const container = document.getElementById(elementId);
  container.innerHTML = "";

  data.forEach((item) => {
    const row = document.createElement("div");
    row.className = "data-row";
    row.innerHTML = `
            <span>${item.address}</span>
            <span>${item.uxuy}</span>
        `;
    container.appendChild(row);
  });
}

// // Disable right-click
// document.addEventListener("contextmenu", (e) => e.preventDefault());

// function ctrlShiftKey(e, keyCode) {
//   return e.ctrlKey && e.shiftKey && e.keyCode === keyCode.charCodeAt(0);
// }

// document.onkeydown = (e) => {
//   // Disable F12, Ctrl + Shift + I, Ctrl + Shift + J, Ctrl + U
//   if (
//     event.keyCode === 123 ||
//     ctrlShiftKey(e, "I") ||
//     ctrlShiftKey(e, "J") ||
//     ctrlShiftKey(e, "C") ||
//     (e.ctrlKey && e.keyCode === "U".charCodeAt(0))
//   )
//     return false;
// };