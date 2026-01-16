$("#desbloq").on("click", function(){
    var input = $("input").val();
    if (input === "babepodcast") {
        $(".album").removeClass("album");
    }
    else{
        alert("Incorrect.");
    }
});