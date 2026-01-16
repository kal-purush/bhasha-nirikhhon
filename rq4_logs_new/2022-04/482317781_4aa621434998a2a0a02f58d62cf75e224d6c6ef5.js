function colorBackground() {
    const colorSelection = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 'a', 'b', 'c', 'd', 'e', 'f'];

    let firstColor = generateRandomColor(colorSelection);
    let secondColor = generateRandomColor(colorSelection);

    document.getElementById('first-color').innerHTML = `#${firstColor}`;
    document.getElementById('second-color').innerHTML = `#${secondColor}`;

    document.body.style.background = `linear-gradient(90deg, #${firstColor}, #${secondColor})`;
    document.body.style.backgroundSize = "170% 170%";
}

function getRandomArrayElement(colorSelection){
    return colorSelection[Math.floor(Math.random()*colorSelection.length)];
}

function generateRandomColor(colorSelection) {
    let generateColorValue = '';
    for (let i = 0; i < 6; i++) {
        generateColorValue += getRandomArrayElement(colorSelection);
    }
    return generateColorValue;
};