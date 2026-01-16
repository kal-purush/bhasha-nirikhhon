$(document).ready(function() { // opens reviews
    $(document).on('click',".product_display", function(){
        if ($("div").hasClass("expanded") === true){
            console.log("You can only expand one")
        }
        else{
            if ($(this).hasClass("expanded")){
                return;
            }
            else {
                $(this).addClass("expanded");
                $(this).parent().addClass("expandedParent");
                let v = $('<input type="button" value="Close Reviews" class="modifyButtons" onclick="closeEdit()" style="color:white;"/>');
                let t = $('<p class="modifyButtons">This is a test</p>')
                $(this).after().append(v);
                $(this).after().append(t);
            }
        }
    })
})


function closeEdit(){   // closes the reviews
    $(document).remove("modifyButtons");
    $(".modifyButtons").remove();
    $("div").removeClass("expanded");
    $("article").removeClass("expandedParent");
};