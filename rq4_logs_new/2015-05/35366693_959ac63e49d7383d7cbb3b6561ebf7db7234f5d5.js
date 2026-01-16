/**
 * Created by Luzius on 25.05.2015.
 */

var submenus = document.getElementsByClassName("sub-menu");
[].slice.call(submenus).forEach(function(submenu) {
    submenu.onclick = function() {
        alert("clicked");
    }

})


dynamicMenu = {
    createDynamicMenu : function cri(menuItems) {
        var parent = document.createElement("div");
        var menu = document.createElement("ul");
        parent.appendChild(menu);
        menuItems.forEach(function (menuItemText) {
            var menuItem = document.createElement("li");
            menuItem.style.listStyleType = "none";
            menu.appendChild(menuItem);
            menuItem.innerHTML = menuItemText;

            function onmouseenter(event) {
                menuItem.style.fontWeight = "bolder";
                var submenu = cri(menuItems);
                menuItem.appendChild(submenu);
            }

            function onmouseleave(event) {
                menuItem.style.fontWeight = "lighter";
            }


            menuItem.addEventListener("mouseenter", onmouseenter);
            menuItem.addEventListener("mouseleave", onmouseleave);




            })

        return parent;
    }

}


function TestDynamicMenu() {
    var menu = dynamicMenu.createDynamicMenu(["Eins", "Zwei", "Drei"])
    var body = document.getElementsByTagName("body")[0];

    body.appendChild(menu);
}

TestDynamicMenu();