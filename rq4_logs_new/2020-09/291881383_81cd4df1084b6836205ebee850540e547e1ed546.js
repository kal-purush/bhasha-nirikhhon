function greeter(params) {
    return "hello, " + params;
}
var user = "Jane User!";
document.body.innerHTML = greeter(user);