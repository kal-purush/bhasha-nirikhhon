// const btnOne = document.querySelector("#btnOne");
const output = document.querySelector(".calc__output");
const btnNums = document.querySelectorAll(".calc__row__btnNum");
const btnOps = document.querySelectorAll(".calc__row__btnOps");
const btnClear = document.querySelector("#btnClear");
const btnDel = document.querySelector("#btnDel");
const btnEquals = document.querySelector("#btnEquals");
const btnDot = document.querySelector("#btnDot");
// const allBtns = document.querySelectorAll(".calc__row__btn");
let currentOut = "";
let currentTot = 0;
let evalArr = [];
let prevEvalCheck = false;
// let prevOpCheck = false;
let currVal;
let lastVal;

// allBtns.forEach((btn) =>
//     btn.addEventListener("click", () => {
//         console.log("success");
//     }),
// );

btnDot.addEventListener("click", () => {
    if (prevEvalCheck) {
        currentOut = "";
        prevEvalCheck = false;
    }
    evalArr = currentOut.split(/([-+×÷]+)/);
    currVal = currentOut[currentOut.length - 1];
    if (currVal === ".") currentOut = currentOut.slice(0, -1);
    if (evalArr[evalArr.length - 1].includes(".")) return;
    // console.log("dot", currentOut);
    currentOut += btnDot.innerHTML;
    dispOutput();
});

// Digit buttons
for (let i = 0; i < btnNums.length; i++) {
    btnNums[i].addEventListener("click", () => {
        if (prevEvalCheck) {
            currentOut = "";
            prevEvalCheck = false;
        }
        // if (prevOpCheck) prevOpCheck = false;
        currentOut += btnNums[i].innerHTML;
        dispOutput();
    });
}

const dispOutput = () => {
    output.innerHTML = currentOut;
};

// AC - all clear button
btnClear.addEventListener("click", () => {
    currentOut = "";
    dispOutput();
});

// Delete key
btnDel.addEventListener("click", () => {
    currentOut = currentOut.slice(0, -1);
    dispOutput();
});

// Operations buttons
for (let i = 0; i < btnOps.length; i++) {
    btnOps[i].addEventListener("click", () => {
        if (prevEvalCheck) prevEvalCheck = false; // stops new numbers from clearing the output after an eval and then an operation
        currVal = currentOut[currentOut.length - 1];
        lastVal = currentOut[currentOut.length - 2];

        if (/[-]/.test(currVal) && /[+×÷]/.test(lastVal)) {
            return;
        } else if (/[-]/.test(currVal) && /[0-9]/.test(lastVal)) {
            currentOut = currentOut.slice(0, -1);
            console.log("success 2", lastVal, currVal);
        } else if (/[+×÷]/.test(currVal) && /[+×÷]/.test(btnOps[i].innerHTML)) {
            currentOut = currentOut.slice(0, -1);
            console.log("success 3", lastVal, currVal);
        }

        // prevOpCheck = true;
        // currentTot = currentOut;
        currentOut += btnOps[i].innerHTML;
        dispOutput();
    });
}

btnEquals.addEventListener("click", () => {
    evalArr = currentOut.split(/([-+×÷]+)/); // splits the current eval into an array separated by the operations

    if (evalArr[0] === "") evalArr.splice(0, 1, "0"); // adds a zero to the start of the array for expressions starting with a negative number

    currentTot = evalArr[0];
    for (let i = 1; i < evalArr.length; i += 2) {
        currentTot = myEval(parseFloat(currentTot), parseFloat(evalArr[i + 1]), evalArr[i]);
    }

    currentOut = +currentTot.toPrecision(10); // plus at the front converts the result back into a number and removes extra zeros
    // currentOut = currentTot;
    prevEvalCheck = true; // indicates that equals has been pressed
    dispOutput();
});

const myEval = (n, m, operation) => {
    if (operation[1] === "-") {
        m = -m;
        operation = operation.slice(0, -1);
    }
    if (operation === "+") return n + m;
    if (operation === "-") return n - m;
    if (operation === "×") return n * m;
    if (operation === "÷") return n / m;
};

document.addEventListener("keydown", (e) => {
    // console.log(e.key);
    if (e.key === "0") btnNums[9].click();
    if (e.key === "1") btnNums[6].click();
    if (e.key === "2") btnNums[7].click();
    if (e.key === "3") btnNums[8].click();
    if (e.key === "4") btnNums[3].click();
    if (e.key === "5") btnNums[4].click();
    if (e.key === "6") btnNums[5].click();
    if (e.key === "7") btnNums[0].click();
    if (e.key === "8") btnNums[1].click();
    if (e.key === "9") btnNums[2].click();

    if (e.key === ".") btnDot.click();
    if (e.key === "Backspace") btnDel.click();
    if (e.key === "Delete") btnClear.click();

    if (e.key === "/") btnOps[0].click();
    if (e.key === "*") btnOps[1].click();
    if (e.key === "-") btnOps[2].click();
    if (e.key === "+") btnOps[3].click();
    if (e.key === "=" || e.key === "Enter") btnEquals.click();
});

// bugs
// -pressing a digit then dot twice and another digit removes the dot
// -negative numbers sometimes break with keyboard only?