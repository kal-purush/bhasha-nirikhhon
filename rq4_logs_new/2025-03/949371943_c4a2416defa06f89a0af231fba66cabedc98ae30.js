let squares = document.querySelectorAll(".square");
let activePlayer = 0;


const winPatterns = [
    [0, 1, 2],
    [0, 3, 6],
    [0, 4, 8],
    [1, 4, 7],
    [2, 5, 8],
    [2, 4, 6],
    [3, 4, 5],
    [6, 7, 8]
];

squares.forEach((square, index) => {
    square.addEventListener('click', function(event) {
        const button = event.target;
        const parentElement = event.target.parentElement;
        const grandParentElement = event.target.parentElement.parentElement;
        const buttonRelativeIndex = (index - (9 * Array.from(event.target.parentElement.parentElement.children).indexOf(event.target.parentElement)));

        handleClick(button, buttonRelativeIndex, parentElement, grandParentElement);
    });
});

function handleClick(button, buttonRelativeIndex, parentElement, grandParentElement) {

    
    if (activePlayer == 0) {

        button.textContent = "O";
        activePlayer += 1;

    } else {

        button.textContent = "X";
        activePlayer -= 1;
        
    }

    button.classList.add("square-disabled");
    grandParentElementChildren = Array.from(grandParentElement.children)
    grandParentElementChildren.forEach((child, index) => {

        if (index != buttonRelativeIndex) {

            child.classList.add("disabled");

        } else {

            child.classList.remove("disabled");

        }

    });
}