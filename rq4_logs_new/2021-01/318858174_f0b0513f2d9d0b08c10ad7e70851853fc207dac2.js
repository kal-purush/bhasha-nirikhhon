$(document).ready(function(){

	    api("The+Drug+in+me+is+Reimagined",'#f1',"Falling+In+Reverse","#f1_inner","#f1_inner_p")
	    api("Paradise",'#f2',"Coldplay","#f2_inner","#f2_inner_p")
	    api("Lithium","#f3","Evanescence","#f3_inner","#f3_inner_p")
	    api("Hero","#f4","NickelBack","#f4_inner","#f4_inner_p")
	    api("Slow+Down","#f5","Maverick+Sabre","#f5_inner","#f5_inner_p")
	    api("All+that+you+can't+leave+behind","#f6","U2","#f6_inner","#f6_inner_p")
	    api("Lemonade","#f7","Internet+money","#f7_inner","#f7_inner_p")
	    api("vão+dar+banhó+cão","#f8","Iris","#f8_inner","#f8_inner_p")
	    api("You+and+i","#f9","Scorpions","#f9_inner","#f9_inner_p")
	    api("Saint+Pablo","#f10","Kanye+West","#f10_inner","#f10_inner_p")
})



function api (nome,image,banda,h3,p){


	$.ajax({
	        method: "GET",
	        url: "http://ws.audioscrobbler.com/2.0/?method=album.getinfo&api_key=cb5873017d844ca90043a3eae82f9055&artist="+banda+"&album="+nome+"&format=json"
	    }).done(function(album){

	        $.each(album, function(key, value){

	            $(image).attr("src",value.image[3]['#text'])
	           

	            

        });		

	})

}


