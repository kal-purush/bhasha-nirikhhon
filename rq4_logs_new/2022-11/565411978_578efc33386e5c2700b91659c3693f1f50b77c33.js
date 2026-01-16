const menu = document.getElementById("menu");

Array.from(document.getElementsByClassName("menu-item"))
    .forEach((item, index) => {
        item.onmouseover = () => {
            menu.dataset.activeIndex = index;
        }
    });

    const toggleNav = () => {
        document.body.dataset.nav = document.body.dataset.nav === "true" ? "false" : "true";
    };