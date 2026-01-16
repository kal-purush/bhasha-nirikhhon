$(document).ready(function() {
    $('#lang-select').click(function() {
    	var langcontent = $(".lang-select-content").html()
    	console.log(langcontent)
    	var lanp = document.createElement("div")
    	lanp.innerHTML = langcontent
    	console.log(lanp)
        swal({
        	title: "Language",
        	button: false,
            content: lanp,
        });
    })
})