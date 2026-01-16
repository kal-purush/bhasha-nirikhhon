$(document).ready(function(){
    $("#user-menu-button").click(function() {
        var menu = $("[role=menu]")
        menu.hasClass("opacity-0") ?
            menu.removeClass("opacity-0 scale-95").addClass("opacity-100 scale-100 ease-in duration-75")
             :
            menu.removeClass("opacity-100 scale-100").addClass("opacity-0 scale-95 ease-out duration-100")
    })
});