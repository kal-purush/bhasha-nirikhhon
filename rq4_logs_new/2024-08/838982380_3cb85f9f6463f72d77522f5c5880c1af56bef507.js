let colorButton = document.getElementById("color-button");

function changeBackgroundColor() {
    document.body.style.backgroundColor = 'black';
}

colorButton.addEventListener("click", changeBackgroundColor);