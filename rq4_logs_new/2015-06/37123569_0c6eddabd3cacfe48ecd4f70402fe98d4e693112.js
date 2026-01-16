(function(){
	
	var app = angular.module('gemStore', []);
	app.controller('StoreController', function(){
		this.products = gems;
	});

	app.controller('PanelController', function(){
		this.tab = 1;
		this.selectTab = function(setTab){
			this.tab = setTab;
		};
		this.isSelected = function(checkTab){
			return this.tab === checkTab;
		};
	});

	app.controller('ReviewController', function(){
		this.review = {};

		this.addReview = function(product){
			//push the product into this products reviews array
			product.reviews.push(this.review);
			//clear out the form
			this.review = {};
			
		};
	});
// custom directive for including code snippet
	app.directive('productTitle', function(){
		return {
			restrict: 'E',
			templateUrl: 'product-title.html'
		};
	});
	app.directive('productPanels', function(){
		return{
			restrict: 'E',
			templateUrl: 'product-panels.html',
			controller: function(){
						this.tab = 1;
						this.selectTab = function(setTab){
							this.tab = setTab;
						};
						this.isSelected = function(checkTab){
							return this.tab === checkTab;
						};

			},
			controllerAs: 'panel'
		};
	});




	var gems = [
		 {
			name:'Diamond',
			price: 2,
			desc: 'new cut',
			reviews: [
				{
					stars: 5,
					body: "dfvfvefvef",
					author: "keith@gmail.com"

				}


			],
			images: [	
				{
					full: 'img/gem1.jpg',
					thumb: 'img/gem1-thumb.jpg'
				}
			]
		},
		{
			name:'Ruby',
			price: 5.95,
			desc: 'Red Sharp cut',
			reviews: [
				{
					stars: 5,
					body: "dfvfvefvef",
					author: "keith@gmail.com"

				}


			],
			images: [	
				{
					full: 'img/gem2.jpg',
					thumb: 'img/gem2-thumb.jpg'
				}
			]
		}

	];

})();