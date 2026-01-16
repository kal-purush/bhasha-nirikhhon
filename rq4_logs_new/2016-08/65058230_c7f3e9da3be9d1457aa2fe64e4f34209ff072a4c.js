(function(){
	//Create our first module
	var app = angular.module('pokedex', []);

	//Create our first controller
	app.controller('PokemonController', function(){
		this.pokemon = {
			id: 001,
			name : 'Bulbasaur',
			species: 'Seed Pokemon',
			type: ['Grass', "Poison"],
			height: '2.4',
			weight: '15.2 lbs',
			abilities: ['Overgrow', 'Chlorophyll']                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
		};
	});
})();