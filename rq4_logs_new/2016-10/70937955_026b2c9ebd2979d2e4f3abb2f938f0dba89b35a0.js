var searchbar = {

	config : {
		event : 'change' ,
		tagAttribute : 'data-tag' ,
		elements : {
			bar : '[data-search="bar"]' ,
			items : '[data-search="item"]'
		}
	} ,

	init : function(){
		document.querySelector( searchbar.config.elements.bar ).addEventListener( searchbar.config.event , function( e ){
			var searchString = this.value.trim();

			var itemSearch = document.querySelectorAll( searchbar.config.elements.items );
			var qtd = itemSearch.length;

			for (var i = 0; i < qtd; i++) {

				var tag = itemSearch[ i ].getAttribute( searchbar.config.tagAttribute ); 

				if( tag.includes( searchString ) ){
					itemSearch[ i ].style.display = '';
				}
				else{
					itemSearch[ i ].style.display = 'none';
				}

			}
		});
	}
};

searchbar.init();