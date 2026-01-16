function displayMenu() {
    var x = document.getElementsByClassName("drop");
    if (x.style.display === "flex") {
        x.style.display = "none";
    } else {
        x.style.display = "block";
    }
}
