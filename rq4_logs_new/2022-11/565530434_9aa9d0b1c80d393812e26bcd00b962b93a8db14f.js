function add_User() {
    user_name = document.getElementById("inputlogin").value;
    localStorage.setItem("user_name", user_name);
    window.location = "kwitter_room.html"
}