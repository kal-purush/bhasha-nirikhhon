const contBtn = document.getElementById("contact-btn");

contBtn.addEventListener("click", () => {
    document.getElementById("contact").scrollIntoView({block:"start", behavior:"smooth"});
});

const pageTop = document.getElementById("home-button");

pageTop.addEventListener("click", () => {
    document.getElementById("home").scrollIntoView({block:"start", behavior:"smooth"});
});