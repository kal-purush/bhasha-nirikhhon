function myFunction(id) {
    var div = document.getElementById(id);
    console.log(div)
    console.log(id)

    if (div.style.display === "") {
        div.style.display = "block";

    } else {
        div.style.display = "";
    }
}