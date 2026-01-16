var estiloBox = ~function(){
	// recuperar todos os posts
	var post = document.querySelectorAll(".post");
	// alert(post.length);
	function setStylePost(index){
		if(index < 4){
			post[index].style.left = 25*index+"%";
		}
	}
	
	// todos os posts devem ter posição left de: largura x indice
	for(var i = 0; i < post.length; i++){
		setStylePost(i);
	}

}()

mongoimport --db be-mean --collection restaurantes --drop --file restaurantes.json
mongoimport --db be-mean --collection restaurantes --drop --file restaurantes.json

E QUERY    SyntaxError: Unexpected identifier