// Check off specific todos by clicking
$("ul").on("click", "li", function(){
    // if li is grey --> turn it black. else make it grey


    // if ($(this).css("color") == "rgb(128, 128, 128)") {
    //     $(this).css({
    //        color: "black",
    //        textDecoration: "none"
    //     });
    // }
    //
    // else {
    //     $(this).css({
    //        color: "gray",
    //        textDecoration: "line-through"
    //     });
    // }

    $(this).toggleClass("completed");
});

// Click on X to delete To do
$("ul").on("click", "span", function(){
    $(this).parent().fadeOut(function(){
        $(this).remove();
    });
    event.stopPropagation();
})

$("input[type='text']").keypress(function(event){
    if(event.which === 13) {
        // grabbing new todo text
        var todoText = $(this).val();

        // empty the input
        $(this).val("");

        // create new li and add to ul
        $("ul").append("<li><span><i class=\"far fa-trash-alt\"></i> </span> " + todoText +  "</li>")
    }
})

$("#plusSign").click(function() {
    $("input[type='text']").fadeToggle();
});