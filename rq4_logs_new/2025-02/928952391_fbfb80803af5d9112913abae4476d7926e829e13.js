const animationContainer = document.querySelector(".ascii-animation-container");

for (let i = 0; i < 50; i++) {
    const row = document.createElement("div");
    row.classList.add("animation-column");
    animationContainer.appendChild(row)

    for (let i = 0; i < 20; i++) {
        const col = document.createElement("div");
        col.classList.add("block");
        row.appendChild(col)
    }
}

setInterval(randomXY, 100);

function randomXY() {
    const colIndex = Math.floor((Math.random() * 50));
    const rowIndex = Math.floor((Math.random() * 20));
    generateText(rowIndex, colIndex)
}

function generateText(rowIndex, colIndex) {
    const col = document.querySelectorAll(".animation-column")[colIndex]
    const row = col.querySelectorAll(".block")[rowIndex] 

    const x = Math.floor((Math.random() * 94) + 33);
    row.textContent = String.fromCharCode(x)
}