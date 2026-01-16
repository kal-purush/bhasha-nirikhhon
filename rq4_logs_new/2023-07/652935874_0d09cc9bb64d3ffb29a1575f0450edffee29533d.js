const BUTTON = document.querySelector(".toggle");
const SYNC = document.querySelector("#sync");
const TOGGLE = () => {
    const IS_PRESSED = BUTTON.matches("[aria-pressed=true]");
    BUTTON.setAttribute("aria-pressed", IS_PRESSED ? false : true);
    var body = document.getElementById("body");
    var currentClass = body.className;
    body.className = currentClass == "light-mode" ? "dark-mode" : "light-mode";
    localStorage.setItem("emojifyTheme", currentClass);
};
BUTTON.addEventListener("click", TOGGLE);