var _a;
var sum_to_n_a = function (n) {
    var sum = 0;
    for (var i = 1; i <= n; i++) {
        sum += i;
    }
    return sum;
};
var sum_to_n_b = function (n) {
    return (n * (n + 1)) / 2;
};
var sum_to_n_c = function (n) {
    if (n === 1)
        return 1;
    return n + sum_to_n_c(n - 1);
};
(_a = document.getElementById("sumForm")) === null || _a === void 0 ? void 0 : _a.addEventListener("submit", function (e) {
    e.preventDefault();
    var n = parseInt(document.getElementById("number").value, 10);
    var resultsDiv = document.getElementById("results");
    if (resultsDiv) {
        resultsDiv.innerHTML = "\n            <p>Sum (Iterative): ".concat(sum_to_n_a(n), "</p>\n            <p>Sum (Formula): ").concat(sum_to_n_b(n), "</p>\n            <p>Sum (Recursive): ").concat(sum_to_n_c(n), "</p>\n        ");
    }
});