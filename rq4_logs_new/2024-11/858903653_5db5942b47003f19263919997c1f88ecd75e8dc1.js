(function () {
    window.addEventListener("load", function () {

        const loadTime = window.performance.timing.domContentLoadedEventEnd - window.performance.timing.navigationStart;

        const statsElement = document.getElementById("load-stats");
        if (statsElement) {
            statsElement.textContent += `${loadTime} мс`;
        }

        const currentPage = document.location.pathname.split("/").pop().split(".")[0];
        const menuElements = document.querySelectorAll(".nav-element");
        menuElements.forEach(element => {
            if (element.getAttribute("href").includes(currentPage)) {
                element.classList.add("active");
            }
        });
    });
})();