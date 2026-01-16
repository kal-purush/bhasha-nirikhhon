function getHistory() {
        return document.getElementById("history").innerText;
}

function printHistory(num) {
        document.getElementById("history").innerText = num;
}

function getDisplay() {
        return document.getElementById("display").innerText;
}

function printDisplay(num) {
        if (num == "") {
                document.getElementById("display").innerText = num;
        } else {
                document.getElementById("display").innerText = changeinformate(num);
        }
}

function changeinformate(num) {
        if (num == "-") {
                return "0";
        }
        var x = Number(num);
        var y = x.toLocaleString("en-IN");
        return y
}

function removeComa(num) {
        return Number(num.replace(/,/g, ''));
}

var operator = document.getElementsByClassName("operator");
for (i = 0; i < operator.length; i++) {
        operator[i].addEventListener("click", function () {
                if (this.value == "clear") {
                        printDisplay("0");
                        printHistory("");
                } else if (this.value == "backspace") {
                        var display = removeComa(getDisplay()).toString();
                        if (display) {
                                display = display.substr(0, display.length - 1);
                                printDisplay(display);
                                if (display == "")
                                        display = "0";
                                printDisplay(display);
                        }
                } else {
                        var display = getDisplay();
                        var history = getHistory();
                        if (display == "" && history != "") {
                                if (isNaN(history[history.length - 1])) {
                                        history = history.substr(0, history.length - 1);
                                }
                        }
                        if (display != "" || history != "") {
                                display = display == "" ? display : removeComa(display);
                                history = history + display;
                        }
                        if (this.value == "=") {
                                var result = eval(history);
                                printDisplay(result);
                                printHistory("");
                        } else {
                                history = history + this.value;
                                printHistory(history);
                                printDisplay("");
                        }
                }
        });
};
var number = document.getElementsByClassName("number");
for (i = 0; i < number.length; i++) {
        number[i].addEventListener("click", function () {
                var display = removeComa(getDisplay());
                if (display != NaN) {
                        display = display + this.value;
                        printDisplay(display);
                };
        });
};