
var user = {
    username: 't',
    password: '1',
};

function login() {
    var inputUsername = document.getElementById("username").value;
    var inputPassword = document.getElementById("password").value;
    if (!inputUsername || !inputPassword) {
        alert("nhap thong tin");
    }
    else if (
        user.username == inputUsername &&
        user.password == inputPassword
    ) {
        alert("dang nhap thanh cong");
        window.location.href = 'todo/todo.html';
    } else {
        alert("dang nhap that bai");
    }
}