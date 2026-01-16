const container = document.getElementById("container");

const gridSize = 16;

for (let i = 0; i < gridSize * gridSize; i++){
    const createDiv = document.createElement("div");
    createDiv.classList.add("square-box");
    container.appendChild(createDiv);
};

const boxColored = document.querySelectorAll(".square-box");
boxColored.forEach((box) => {
    box.addEventListener("mouseover", () => {
        box.style.backgroundColor = "#4b4b4b";
    });
    
    box.addEventListener("mouseout", () => {
        setTimeout(() => {
            box.style.backgroundColor = "#e4e4e4";
        }, 2000);
    })
});