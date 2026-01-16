const display = document.querySelector("#display");
const buttonsContainer = document.querySelector("#buttons-container");

const buttons = [
    { id: "clear", text: "C", class: "btn-operator" },
    { id: "/", text: "&divide;", class: "btn-operator" },
    { id: "*", text: "&times;", class: "btn-operator" },
    { id: "backspace", text: "<", class: "btn-operator" },
    { id: "7", text: "7", class: "btn-number" },
    { id: "8", text: "8", class: "btn-number" },
    { id: "9", text: "9", class: "btn-number" },
    { id: "-", text: "-", class: "btn-operator" },
    { id: "4", text: "4", class: "btn-number" },
    { id: "5", text: "5", class: "btn-number" },
    { id: "6", text: "6", class: "btn-number" },
    { id: "+", text: "+", class: "btn-operator" },
    { id: "1", text: "1", class: "btn-number" },
    { id: "2", text: "2", class: "btn-number" },
    { id: "3", text: "3", class: "btn-number" },
    { id: "R", text: "R", class: "btn-operator" },
    { id: "(", text: "(", class: "btn-operator" },
    { id: "0", text: "0", class: "btn-number" },
    { id: ")", text: ")", class: "btn-operator" },
    { id: "equal", text: "=", class: "btn-equal" }
];

buttons.forEach((button, index) => {
    const btnElement = document.createElement("button");
    btnElement.id = button.id;
    btnElement.innerHTML = button.text;
    btnElement.className = button.class;

    buttonsContainer.appendChild(btnElement);

    
    if ((index + 1) % 4 === 0) {
        buttonsContainer.appendChild(document.createElement("br"));
    }
});

const buttonElements = document.querySelectorAll("button");

buttonElements.forEach((item) => {
    item.onclick = () => {
        if (item.id == "clear") {
            display.innerText = "";
        } else if (item.id == "backspace") {
            let string = display.innerText.toString();
            display.innerText = string.substr(0, string.length - 1);
        } else if (display.innerText != "" && item.id == "equal") {
            display.innerText = eval(display.innerText);
        } else if (display.innerText == "" && item.id == "equal") {
            display.innerText = "Give the Input";
            setTimeout(() => (display.innerText = ""), 2000);
        }
        else if(display.innerText != "" && item.id == "R"){
            display.innerText = Math.random(eval(display.innerText));
        } else {
            display.innerText += item.id;
        }
    };
});

const calculator = document.querySelector(".calculator");