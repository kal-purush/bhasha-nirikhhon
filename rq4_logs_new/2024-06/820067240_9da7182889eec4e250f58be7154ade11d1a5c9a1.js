function addition() {
    let res, x, y;
    x = +num1.value;
    y = +num2.value;
    res = x + y;
    op.value = "Sum" + res;
    alert("Sum=" + res);
}
function substraction() {
    let res, x, y;
    x = +num1.value;
    y = +num2.value;
    res = x - y;

    alert("Sum=" + res);

}
function Multilication() {
    let res, x, y;
    x = +num1.value;
    y = +num2.value;
    res = x * y;

    alert("Sum=" + res);

}
function Division() {
    let res, x, y;
    x = +num1.value;
    y = +num2.value;
    if (x == 0) {
        return "cannot divide by Zero"
    }
    res = x / y;

    alert("Sum=" + res);

}