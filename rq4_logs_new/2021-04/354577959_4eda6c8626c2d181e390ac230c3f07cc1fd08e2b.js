window.onload = function () {
    let btnLogOut = document.getElementById("logout");

    if (btnLogOut != null){
        btnLogOut.addEventListener("click", () => {
            document.getElementById("formLogOut").submit();
        })
    }
}