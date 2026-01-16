//author Maxim Fluieraru @2015


function add_bootstrap () {
		
		var head = document.getElementsByTagName('head')[0];

		var link=document.createElement("link");

        link.href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/css/bootstrap.min.css";
        link.rel="stylesheet";

        head.appendChild(link);
	  
}

add_bootstrap();
         
                    
        