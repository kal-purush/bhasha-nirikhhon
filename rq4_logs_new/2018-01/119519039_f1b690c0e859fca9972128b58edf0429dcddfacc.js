
var app = angular.module('app', ['ngRoute']);


// service
app.service('factoryService', ['$http', '$q',
    function dataService($http, $q) {
        
        var deferredOne = $q.defer();

        var deferredTwo = $q.defer();

        this.getDataOne = function(){
        	return $http.get('https://raw.githubusercontent.com/openfootball/football.json/master/2015-16/en.1.json')
        		.then(function (response){
        			// promise is fulfilled
        			deferredOne.resolve(response.data.rounds);
        			
        			// promise is returned
        			return deferredOne.promise;
        		}, function (response){
        			// the following line rejects the promise
        			deferredOne.reject(response);
        			// promise is returned
        			return deferredOne.promise;
        		});
        }

        this.getDataTwo = function(){
        	return $http.get('https://raw.githubusercontent.com/openfootball/football.json/master/2016-17/en.1.json')
        		.then(function (response){
        			// promise is fulfilled
        			deferredTwo.resolve(response.data.rounds);
        			
        			// promise is returned
        			return deferredTwo.promise;
        		}, function (response){
        			// the following line rejects the promise
        			deferredTwo.reject(response);
        			// promise is returned
        			return deferredTwo.promise;
        		});
        }
    }]);



// Home Controller
app.controller("homeController",  ['factoryService', function(factoryService){
	var main = this;

	main.loadOne = function(){
		factoryService.getDataOne()
		.then(function (result){

			main.dataOne = result;

			for(var i = 0; i < main.dataOne.length; i++){
				main.dataOne[i].idHead = i;
				
				for(var j = 0; j < main.dataOne[i].matches.length; j++){
					main.dataOne[i].matches[j].idTail = j;
				}
			}
			}, function(error){
			console.log(error.statusText);
		});
	}

	main.loadTwo = function(){
		factoryService.getDataTwo()
		.then(function (result){

			main.dataTwo = result;

			for(var i = 0; i < main.dataTwo.length; i++){
				main.dataTwo[i].idHead = i;
				
				for(var j = 0; j < main.dataTwo[i].matches.length; j++){
					main.dataTwo[i].matches[j].idTail = j;	
				}
			}
			}, function(error){
			console.log(error.statusText);
		});
	}

}]);



// SelfController
app.controller("selfController", ['$http', '$routeParams', 'factoryService', function($http,$routeParams,factoryService){

	//create a context
	var main = this;

	this.matchHeadingOne = '';

	this.parentId 	= 	$routeParams.parent;
	this.childId 	= 	$routeParams.child;

	this.bigId 		= 	$routeParams.big;
	this.smallId 	= 	$routeParams.small;

	console.log(this.bigId);
	console.log(this.smallId);

	this.matchOne = '';
	this.matchTwo = '';

	if((this.parentId === null || this.parentId === undefined) || (this.childId === null || this.childId === undefined)){
		this.matchOne = false;
	}
	else{
		this.matchOne = true;
	}

	if((this.bigId === null || this.bigId === undefined) || (this.smallId === null || this.smallId === undefined)){
		this.matchTwo = false;
	}
	else{
		this.matchTwo = true;
	}


	
	this.urlOne = "https://raw.githubusercontent.com/openfootball/football.json/master/2015-16/en.1.json";

	this.urlTwo = "https://raw.githubusercontent.com/openfootball/football.json/master/2016-17/en.1.json";

	
	

	this.loadMatchOne = function(){
		$http({
			method: 'GET',
			url: main.urlOne
		}).then(function successCallback(response){
			main.matchHeadingOne = response.data.rounds[main.parentId].name;
			main.matchDataOne = response.data.rounds[main.parentId].matches[main.childId];
			

			if(main.matchDataOne.score1 === null || main.matchDataOne.score2 === null ){
				
				main.matchResultOne = "Match Cancelled";
				main.cancelledOne = true;
				
				main.valOne = false;
				main.drawOne = false;				
			}
			else if(main.matchDataOne.score1 > main.matchDataOne.score2){
				main.matchResultOne = main.matchDataOne.team1.name;
				
				main.valOne = true;
				main.cancelledOne = false;
				main.drawOne = false; 
			}
			else if(main.matchDataOne.score1 === main.matchDataOne.score2){
				main.matchResultOne = "Draw";
				main.drawOne = true;
				
				main.valOne = false;
				main.cancelledOne = false;
			}
			else{
				main.matchResultOne = main.matchDataOne.team2.name;
				main.valOne = true;
				main.cancelledOne = false;
				main.drawOne = false; 
			}	

		}, function errorCallback(response){
			console.log(response);
		});
	}

	this.loadMatchTwo = function(){
		$http({
			method: 'GET',
			url: main.urlTwo
		}).then(function successCallback(response){
			main.matchHeadingTwo = response.data.rounds[main.bigId].name;
			main.matchDataTwo = response.data.rounds[main.bigId].matches[main.smallId];

	
			if(main.matchDataTwo.score1 === null || main.matchDataTwo.score2 === null ){
				
				main.matchResultTwo = "Match Cancelled";
				main.cancelledTwo = true;
				
			}
			else if(main.matchDataTwo.score1 > main.matchDataTwo.score2){
				main.matchResultTwo = main.matchDataTwo.team1.name;
				main.valTwo = true;
			}
			else if(main.matchDataTwo.score1 === main.matchDataTwo.score2){
				main.matchResultTwo = "Draw";
				main.drawTwo = true;
			}
			else{
				main.matchResultTwo = main.matchDataTwo.team2.name;
				main.valTwo = true;
			}	

		}, function errorCallback(response){
			console.log(response);
		});
	}
}]);




// Statistics Cotroller 1
app.controller("statsOneController", ['$http', '$routeParams', 'factoryService', function($http,$routeParams, factoryService){

	var main = this;
	main.data = [];
	console.log(main.data.length);

	this.idOne = $routeParams.teamNameOne;
	console.log(typeof(this.idOne));


	if(this.idOne === null || this.idOne === undefined){
		this.matchOne = false;
	}
	else{
		this.matchOne = true;
	}
	
	// WIN SCORES
	this.ARSwinOne =0;
	this.AVLwinOne =0;
	this.BOUwinOne =0;
	this.CHEwinOne =0;
	this.CRYwinOne =0;
	this.EVEwinOne =0;
	this.LEIwinOne =0;
	this.LIVwinOne =0;
	this.MCIwinOne =0;
	this.MUNwinOne =0;
	this.NEWwinOne =0;
	this.NORwinOne =0;
	this.SOUwinOne =0;
	this.STKwinOne =0;
	this.SUNwinOne =0;
	this.SWAwinOne =0;
	this.TOTwinOne =0;
	this.WATwinOne =0;
	this.WBAwinOne =0;
	this.WHUwinOne =0;

	// DRAW SCORES
	this.ARSdrawOne = 0;
	this.AVLdrawOne = 0;
	this.BOUdrawOne = 0;
	this.CHEdrawOne = 0;
	this.CRYdrawOne = 0;
	this.EVEdrawOne = 0;
	this.LEIdrawOne = 0;
	this.LIVdrawOne = 0;
	this.MCIdrawOne = 0;
	this.MUNdrawOne = 0;
	this.NEWdrawOne = 0;
	this.NORdrawOne = 0;
	this.SOUdrawOne = 0;
	this.STKdrawOne = 0;
	this.SUNdrawOne = 0;
	this.SWAdrawOne = 0;
	this.TOTdrawOne = 0;
	this.WATdrawOne = 0;
	this.WBAdrawOne = 0;
	this.WHUdrawOne = 0;

	// LOSE MATCHES
	this.ARSloseOne = 0;
	this.AVLloseOne = 0;
	this.BOUloseOne = 0;
	this.CHEloseOne = 0;
	this.CRYloseOne = 0;
	this.EVEloseOne = 0;
	this.LEIloseOne = 0;
	this.LIVloseOne = 0;
	this.MCIloseOne = 0;
	this.MUNloseOne = 0;
	this.NEWloseOne = 0;
	this.NORloseOne = 0;
	this.SOUloseOne = 0;
	this.STKloseOne = 0;
	this.SUNloseOne = 0;
	this.SWAloseOne = 0;
	this.TOTloseOne = 0;
	this.WATloseOne = 0;
	this.WBAloseOne = 0;
	this.WHUloseOne = 0;


	// vars for gf and gas
	this.scoreARSOne = 0;
	this.onARSOne = 0;

	this.scoreAVLOne = 0;
	this.onAVLOne = 0;

	this.scoreBOUOne = 0;
	this.onBOUOne = 0;

	this.scoreCHEOne = 0;
	this.onCHEOne = 0;

	this.scoreCRYOne = 0;
	this.onCRYOne = 0;

	this.scoreEVEOne = 0;
	this.onEVEOne = 0;

	this.scoreLEIOne = 0;
	this.onLEIOne = 0;

	this.scoreLIVOne = 0;
	this.onLIVOne = 0;

	this.scoreMCIOne = 0;
	this.onMCIOne = 0;

	this.scoreMUNOne = 0;
	this.onMUNOne = 0;

	this.scoreNEWOne = 0;
	this.onNEWOne = 0;

	this.scoreNOROne = 0;
	this.onNOROne = 0;

	this.scoreSOUOne = 0;
	this.onSOUOne = 0;

	this.scoreSTKOne = 0;
	this.onSTKOne = 0;

	this.scoreSUNOne = 0;
	this.onSUNOne = 0;

	this.scoreSWAOne = 0;
	this.onSWAOne = 0;

	this.scoreTOTOne = 0;
	this.onTOTOne = 0;

	this.scoreWATOne = 0;
	this.onWATOne = 0;

	this.scoreWBAOne = 0;
	this.onWBAOne = 0;

	this.scoreWHUOne = 0;
	this.onWHUOne = 0;
	
	
	// 2015-16 data
	main.loadOne = function(id){
		
		factoryService.getDataOne()
		.then(function (result){
			// promise was fullfilled (regardless of outcome)
			// checks for information will be performed here
			main.dataOne = result;
			console.log(main.dataOne);
			
			main.teamName = function(value){
				switch (value){
					case 'ARS':
						for(var i = 0; i < main.dataOne.length; i++){	
							for(var j = 0; j < main.dataOne[i].matches.length; j++){
								// TEAM ARS
								if(main.dataOne[i].matches[j].team1.code === "ARS"){
									main.nameOne = main.dataOne[i].matches[j].team1.name;
									main.scoreARSOne += main.dataOne[i].matches[j].score1;
									
									main.onARSOne += main.dataOne[i].matches[j].score2;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										main.ARSwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.ARSloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.ARSdrawOne++;
										
									}
								}
								else if(main.dataOne[i].matches[j].team2.code === "ARS"){
									main.scoreARSOne += main.dataOne[i].matches[j].score2;

									main.onARSOne += main.dataOne[i].matches[j].score1;
									
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										
										main.ARSloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.ARSwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.ARSdrawOne++;
										
									}
								}	
							}
						}
						return [main.ARSwinOne, main.ARSloseOne, main.ARSdrawOne, main.scoreARSOne, main.onARSOne, main.nameOne];
						break;

					case 'AVL':
						for(var i = 0; i < main.dataOne.length; i++){
							console.log(main.dataOne[i].name);
							for(var j = 0; j < main.dataOne[i].matches.length; j++){
								// TEAM AVL
								if(main.dataOne[i].matches[j].team1.code === "AVL"){
									main.nameOne = main.dataOne[i].matches[j].team1.name;
									main.scoreAVLOne += main.dataOne[i].matches[j].score1;
									
									main.onAVLOne += main.dataOne[i].matches[j].score2;	
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										main.AVLwinOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.AVLloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.AVLdrawOne++;
										
									}	
								}
								else if(main.dataOne[i].matches[j].team2.code === "AVL"){
									main.scoreAVLOne += main.dataOne[i].matches[j].score2;

									main.onAVLOne += main.dataOne[i].matches[j].score1;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										
										main.AVLloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.AVLwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 === null && main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.AVLdrawOne++;
										
									}
								}
							}
						}
						return [main.AVLwinOne, main.AVLloseOne, main.AVLdrawOne, main.scoreAVLOne, main.onAVLOne, main.nameOne];
						break;
						

					case 'BOU':
						for(var i = 0; i < main.dataOne.length; i++){
							
							for(var j = 0; j < main.dataOne[i].matches.length; j++){
								// TEAM BOU
								if(main.dataOne[i].matches[j].team1.code === "BOU"){
									main.nameOne = main.dataOne[i].matches[j].team1.name;
									main.scoreBOUOne += main.dataOne[i].matches[j].score1;
									
									main.onBOUOne += main.dataOne[i].matches[j].score2;	
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										main.BOUwinOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.BOUloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.BOUdrawOne++;
										
									}	
								}
								else if(main.dataOne[i].matches[j].team2.code === "BOU"){
									main.scoreBOUOne += main.dataOne[i].matches[j].score2;

									main.onBOUOne += main.dataOne[i].matches[j].score1;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										
										main.BOUloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.BOUwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 === null && main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.BOUdrawOne++;
										
									}
								}
							}
						}
						return [main.BOUwinOne, main.BOUloseOne, main.BOUdrawOne, main.scoreBOUOne, main.onBOUOne, main.nameOne];
						break;
						

					case 'CHE':
						for(var i = 0; i < main.dataOne.length; i++){
							
							for(var j = 0; j < main.dataOne[i].matches.length; j++){
								// TEAM CHE
								if(main.dataOne[i].matches[j].team1.code === "CHE"){
									main.nameOne = main.dataOne[i].matches[j].team1.name;
									main.scoreCHEOne += main.dataOne[i].matches[j].score1;
									
									main.onCHEOne += main.dataOne[i].matches[j].score2;	
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										main.CHEwinOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.CHEloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.CHEdrawOne++;
										
									}	
								}
								else if(main.dataOne[i].matches[j].team2.code === "CHE"){
									main.scoreCHEOne += main.dataOne[i].matches[j].score2;

									main.onCHEOne += main.dataOne[i].matches[j].score1;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										
										main.CHEloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.CHEwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 === null && main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.CHEdrawOne++;
										
									}
								}
							}
						}
						return [main.CHEwinOne, main.CHEloseOne, main.CHEdrawOne, main.scoreCHEOne, main.onCHEOne, main.nameOne];
						break;
						

					case 'CRY':
						for(var i = 0; i < main.dataOne.length; i++){
							
							for(var j = 0; j < main.dataOne[i].matches.length; j++){
								// TEAM CRY
								if(main.dataOne[i].matches[j].team1.code === "CRY"){
									main.nameOne = main.dataOne[i].matches[j].team1.name;	
									main.scoreCRYOne += main.dataOne[i].matches[j].score1;
									
									main.onCRYOne += main.dataOne[i].matches[j].score2;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										main.CRYwinOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.CRYloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.CRYdrawOne++;
										
									}	
								}
								else if(main.dataOne[i].matches[j].team2.code === "CRY"){
									main.scoreCRYOne += main.dataOne[i].matches[j].score2;

									main.onCRYOne += main.dataOne[i].matches[j].score1;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										
										main.CRYloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.CRYwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 === null && main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.CRYdrawOne++;
										
									}
								}
							}
						}
						return [main.CRYwinOne, main.CRYloseOne, main.CRYdrawOne, main.scoreCRYOne, main.onCRYOne, main.nameOne];
						break;
						

					case 'EVE':
						for(var i = 0; i < main.dataOne.length; i++){
							
							for(var j = 0; j < main.dataOne[i].matches.length; j++){
								// TEAM EVE
								if(main.dataOne[i].matches[j].team1.code === "EVE"){
									main.nameOne = main.dataOne[i].matches[j].team1.name;	
									main.scoreEVEOne += main.dataOne[i].matches[j].score1;
									
									main.onEVEOne += main.dataOne[i].matches[j].score2;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										main.EVEwinOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.EVEloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.EVEdrawOne++;
										
									}	
								}
								else if(main.dataOne[i].matches[j].team2.code === "EVE"){
									main.scoreEVEOne += main.dataOne[i].matches[j].score2;

									main.onEVEOne += main.dataOne[i].matches[j].score1;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										
										main.EVEloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.EVEwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 === null && main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.EVEdrawOne++;
										
									}
								}
							}
						}
						return [main.EVEwinOne, main.EVEloseOne, main.EVEdrawOne, main.scoreEVEOne, main.onEVEOne, main.nameOne];
						break;
						

					case 'LEI':
						for(var i = 0; i < main.dataOne.length; i++){
							
							for(var j = 0; j < main.dataOne[i].matches.length; j++){
								// TEAM LEI
								if(main.dataOne[i].matches[j].team1.code === "LEI"){
									main.nameOne = main.dataOne[i].matches[j].team1.name;	
									main.scoreLEIOne += main.dataOne[i].matches[j].score1;
									
									main.onLEIOne += main.dataOne[i].matches[j].score2;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										main.LEIwinOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.LEIloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.LEIdrawOne++;
										
									}	
								}
								else if(main.dataOne[i].matches[j].team2.code === "LEI"){
									main.scoreLEIOne += main.dataOne[i].matches[j].score2;

									main.onLEIOne += main.dataOne[i].matches[j].score1;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										
										main.LEIloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.LEIwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 === null && main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.LEIdrawOne++;
										
									}
								}
							}
						}
						return [main.LEIwinOne, main.LEIloseOne, main.LEIdrawOne, main.scoreLEIOne, main.onLEIOne, main.nameOne];
						break;
						

					case 'LIV':
						for(var i = 0; i < main.dataOne.length; i++){
							
							for(var j = 0; j < main.dataOne[i].matches.length; j++){
								// TEAM LIV
								if(main.dataOne[i].matches[j].team1.code === "LIV"){	
									main.nameOne = main.dataOne[i].matches[j].team1.name;
									main.scoreLIVOne += main.dataOne[i].matches[j].score1;
									
									main.onLIVOne += main.dataOne[i].matches[j].score2;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										main.LIVwinOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.LIVloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.LIVdrawOne++;
										
									}	
								}
								else if(main.dataOne[i].matches[j].team2.code === "LIV"){
									main.scoreLIVOne += main.dataOne[i].matches[j].score2;

									main.onLIVOne += main.dataOne[i].matches[j].score1;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										
										main.LIVloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.LIVwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 === null && main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.LIVdrawOne++;
										
									}
								}
							}
						}
						return [main.LIVwinOne, main.LIVloseOne, main.LIVdrawOne, main.scoreLIVOne, main.onLIVOne, main.nameOne];
						break;
						

					case 'MCI':
						for(var i = 0; i < main.dataOne.length; i++){
							
							for(var j = 0; j < main.dataOne[i].matches.length; j++){
								// TEAM MCI
								if(main.dataOne[i].matches[j].team1.code === "MCI"){	
									main.nameOne = main.dataOne[i].matches[j].team1.name;
									main.scoreMCIOne += main.dataOne[i].matches[j].score1;
									
									main.onMCIOne += main.dataOne[i].matches[j].score2;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										main.MCIwinOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.MCIloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.MCIdrawOne++;
										
									}	
								}
								else if(main.dataOne[i].matches[j].team2.code === "MCI"){
									main.scoreMCIOne += main.dataOne[i].matches[j].score2;

									main.onMCIOne += main.dataOne[i].matches[j].score1;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										
										main.MCIloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.MCIwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 === null && main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.MCIdrawOne++;
										
									}
								}
							}
						}
						return [main.MCIwinOne, main.MCIloseOne, main.MCIdrawOne, main.scoreMCIOne, main.onMCIOne, main.nameOne];
						break;
						

					case 'MUN':
						for(var i = 0; i < main.dataOne.length; i++){
							
							for(var j = 0; j < main.dataOne[i].matches.length; j++){
								// TEAM MUN
								if(main.dataOne[i].matches[j].team1.code === "MUN"){	
									main.nameOne = main.dataOne[i].matches[j].team1.name;
									main.scoreMUNOne += main.dataOne[i].matches[j].score1;
									
									main.onMUNOne += main.dataOne[i].matches[j].score2;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										main.MUNwinOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.MUNloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.MUNdrawOne++;
										
									}	
								}
								else if(main.dataOne[i].matches[j].team2.code === "MUN"){
									main.scoreMUNOne += main.dataOne[i].matches[j].score2;

									main.onMUNOne += main.dataOne[i].matches[j].score1;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										
										main.MUNloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.MUNwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 === null && main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.MUNdrawOne++;
										
									}
								}
							}
						}
						return [main.MUNwinOne, main.MUNloseOne, main.MUNdrawOne, main.scoreMUNOne, main.onMUNOne, main.nameOne];
						break;
						

					case 'NEW':
						for(var i = 0; i < main.dataOne.length; i++){
							
							for(var j = 0; j < main.dataOne[i].matches.length; j++){
								// TEAM NEW
								if(main.dataOne[i].matches[j].team1.code === "NEW"){	
									main.nameOne = main.dataOne[i].matches[j].team1.name;
									main.scoreNEWOne += main.dataOne[i].matches[j].score1;
									
									main.onNEWOne += main.dataOne[i].matches[j].score2;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										main.NEWwinOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.NEWloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.NEWdrawOne++;
										
									}	
								}
								else if(main.dataOne[i].matches[j].team2.code === "NEW"){
									main.scoreNEWOne += main.dataOne[i].matches[j].score2;

									main.onNEWOne += main.dataOne[i].matches[j].score1;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										
										main.NEWloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.NEWwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 === null && main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.NEWdrawOne++;
										
									}
								}
							}
						}
						return [main.NEWwinOne, main.NEWloseOne, main.NEWdrawOne, main.scoreNEWOne, main.onNEWOne, main.nameOne];
						break;
						

					case 'NOR':
						for(var i = 0; i < main.dataOne.length; i++){
							
							for(var j = 0; j < main.dataOne[i].matches.length; j++){
								// TEAM NOR
								if(main.dataOne[i].matches[j].team1.code === "NOR"){	
									main.nameOne = main.dataOne[i].matches[j].team1.name;
									main.scoreNOROne += main.dataOne[i].matches[j].score1;
									
									main.onNOROne += main.dataOne[i].matches[j].score2;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										main.NORwinOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.NORloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.NORdrawOne++;
										
									}	
								}
								else if(main.dataOne[i].matches[j].team2.code === "NOR"){
									main.scoreNOROne += main.dataOne[i].matches[j].score2;

									main.onNOROne += main.dataOne[i].matches[j].score1;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										
										main.NORloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.NORwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 === null && main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.NORdrawOne++;
										
									}
								}
							}
						}
						return [main.NORwinOne, main.NORloseOne, main.NORdrawOne, main.scoreNOROne, main.onNOROne, main.nameOne];
						break;
						

					case 'SOU':
						for(var i = 0; i < main.dataOne.length; i++){
							
							for(var j = 0; j < main.dataOne[i].matches.length; j++){
								// TEAM SOU
								if(main.dataOne[i].matches[j].team1.code === "SOU"){	
									main.nameOne = main.dataOne[i].matches[j].team1.name;
									main.scoreSOUOne += main.dataOne[i].matches[j].score1;
									
									main.onSOUOne += main.dataOne[i].matches[j].score2;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										main.SOUwinOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.SOUloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.SOUdrawOne++;
										
									}	
								}
								else if(main.dataOne[i].matches[j].team2.code === "SOU"){
									main.scoreSOUOne += main.dataOne[i].matches[j].score2;

									main.onSOUOne += main.dataOne[i].matches[j].score1;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										
										main.SOUloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.SOUwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 === null && main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.SOUdrawOne++;
										
									}
								}
							}
						}
						return [main.SOUwinOne, main.SOUloseOne, main.SOUdrawOne, main.scoreSOUOne, main.onSOUOne, main.nameOne];
						break;
						

					case 'STK':
						for(var i = 0; i < main.dataOne.length; i++){
							
							for(var j = 0; j < main.dataOne[i].matches.length; j++){
								// TEAM STK
								if(main.dataOne[i].matches[j].team1.code === "STK"){	
									main.nameOne = main.dataOne[i].matches[j].team1.name;
									main.scoreSTKOne += main.dataOne[i].matches[j].score1;
									
									main.onSTKOne += main.dataOne[i].matches[j].score2;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										main.STKwinOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.STKloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.STKdrawOne++;
										
									}	
								}
								else if(main.dataOne[i].matches[j].team2.code === "STK"){
									main.scoreSTKOne += main.dataOne[i].matches[j].score2;

									main.onSTKOne += main.dataOne[i].matches[j].score1;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										
										main.STKloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.STKwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 === null && main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.STKdrawOne++;
										
									}
								}
							}
						}
						return [main.STKwinOne, main.STKloseOne, main.STKdrawOne, main.scoreSTKOne, main.onSTKOne, main.nameOne];
						break;
						

					case 'SUN':
						for(var i = 0; i < main.dataOne.length; i++){
							
							for(var j = 0; j < main.dataOne[i].matches.length; j++){
								// TEAM SUN
								if(main.dataOne[i].matches[j].team1.code === "SUN"){	
									main.nameOne = main.dataOne[i].matches[j].team1.name;
									main.scoreSUNOne += main.dataOne[i].matches[j].score1;
									
									main.onSUNOne += main.dataOne[i].matches[j].score2;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										main.SUNwinOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.SUNloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.SUNdrawOne++;
										
									}	
								}
								else if(main.dataOne[i].matches[j].team2.code === "SUN"){
									main.scoreSUNOne += main.dataOne[i].matches[j].score2;

									main.onSUNOne += main.dataOne[i].matches[j].score1;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										
										main.SUNloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.SUNwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 === null && main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.SUNdrawOne++;
										
									}
								}
							}
						}
						return [main.SUNwinOne, main.SUNloseOne, main.SUNdrawOne, main.scoreSUNOne, main.onSUNOne, main.nameOne];
						break;
						

					case 'SWA':
						for(var i = 0; i < main.dataOne.length; i++){
							
							for(var j = 0; j < main.dataOne[i].matches.length; j++){
								// TEAM SWA
								if(main.dataOne[i].matches[j].team1.code === "SWA"){	
									main.nameOne = main.dataOne[i].matches[j].team1.name;
									main.scoreSWAOne += main.dataOne[i].matches[j].score1;
									
									main.onSWAOne += main.dataOne[i].matches[j].score2;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										main.SWAwinOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.SWAloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.SWAdrawOne++;
										
									}	
								}
								else if(main.dataOne[i].matches[j].team2.code === "SWA"){
									main.scoreSWAOne += main.dataOne[i].matches[j].score2;

									main.onSWAOne += main.dataOne[i].matches[j].score1;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										
										main.SWAloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.SWAwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 === null && main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.SWAdrawOne++;
										
									}
								}
							}
						}
						return [main.SWAwinOne, main.SWAloseOne, main.SWAdrawOne, main.scoreSWAOne, main.onSWAOne, main.nameOne];
						break;
						

					case 'TOT':
						for(var i = 0; i < main.dataOne.length; i++){
							
							for(var j = 0; j < main.dataOne[i].matches.length; j++){
								// TEAM TOT
								if(main.dataOne[i].matches[j].team1.code === "TOT"){	
									main.nameOne = main.dataOne[i].matches[j].team1.name;
									main.scoreTOTOne += main.dataOne[i].matches[j].score1;
									
									main.onTOTOne += main.dataOne[i].matches[j].score2;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										main.TOTwinOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.TOTloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.TOTdrawOne++;
										
									}	
								}
								else if(main.dataOne[i].matches[j].team2.code === "TOT"){
									main.scoreTOTOne += main.dataOne[i].matches[j].score2;

									main.onTOTOne += main.dataOne[i].matches[j].score1;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										
										main.TOTloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.TOTwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 === null && main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.TOTdrawOne++;
										
									}
								}
							}
						}
						return [main.TOTwinOne, main.TOTloseOne, main.TOTdrawOne, main.scoreTOTOne, main.onTOTOne, main.nameOne];
						break;
						

					case 'WAT':
						for(var i = 0; i < main.dataOne.length; i++){
							
							for(var j = 0; j < main.dataOne[i].matches.length; j++){
								// TEAM WAT
								if(main.dataOne[i].matches[j].team1.code === "WAT"){	
									main.nameOne = main.dataOne[i].matches[j].team1.name;
									main.scoreWATOne += main.dataOne[i].matches[j].score1;
									
									main.onWATOne += main.dataOne[i].matches[j].score2;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										main.WATwinOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.WATloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.WATdrawOne++;
										
									}	
								}
								else if(main.dataOne[i].matches[j].team2.code === "WAT"){
									main.scoreWATOne += main.dataOne[i].matches[j].score2;

									main.onWATOne += main.dataOne[i].matches[j].score1;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										
										main.WATloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.WATwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 === null && main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.WATdrawOne++;
										
									}
								}
							}
						}
						return [main.WATwinOne, main.WATloseOne, main.WATdrawOne, main.scoreWATOne, main.onWATOne, main.nameOne];
						break;
						

					case 'WBA':
						for(var i = 0; i < main.dataOne.length; i++){
							
							for(var j = 0; j < main.dataOne[i].matches.length; j++){
								// TEAM WBA
								if(main.dataOne[i].matches[j].team1.code === "WBA"){	
									main.nameOne = main.dataOne[i].matches[j].team1.name;
									main.scoreWBAOne += main.dataOne[i].matches[j].score1;
									
									main.onWBAOne += main.dataOne[i].matches[j].score2;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										main.WBAwinOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										main.WBAloseOne++;
										
										
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.WBAdrawOne++;
										
									}	
								}
								else if(main.dataOne[i].matches[j].team2.code === "WBA"){
									main.scoreWBAOne += main.dataOne[i].matches[j].score2;

									main.onWBAOne += main.dataOne[i].matches[j].score1;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										
										main.WBAloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.WBAwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 === null && main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.WBAdrawOne++;
										
									}
								}
							}
						}
						return [main.WBAwinOne, main.WBAloseOne, main.WBAdrawOne, main.scoreWBAOne, main.onWBAOne, main.nameOne];
						break;
						

					case 'WHU':
						for(var i = 0; i < main.dataOne.length; i++){
							
							for(var j = 0; j < main.dataOne[i].matches.length; j++){
								// TEAM WHU
								if(main.dataOne[i].matches[j].team1.code === "WHU"){	
									main.nameOne = main.dataOne[i].matches[j].team1.name;
									main.scoreWHUOne += main.dataOne[i].matches[j].score1;
									
									main.onWHUOne += main.dataOne[i].matches[j].score2;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										main.WHUwinOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										main.WHUloseOne++;
										
										
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.WHUdrawOne++;
										
									}	
								}
								else if(main.dataOne[i].matches[j].team2.code === "WHU"){
									main.scoreWHUOne += main.dataOne[i].matches[j].score2;

									main.onWHUOne += main.dataOne[i].matches[j].score1;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										
										main.WHUloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.WHUwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 === null && main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled");
									}
									else{
										
										main.WHUdrawOne++;
										
									}
								}
							}
						}
						return [main.WHUwinOne, main.WHUloseOne, main.WHUdrawOne, main.scoreWHUOne, main.onWHUOne, main.nameOne];
						break;	
						
				}
			}

		main.a = main.teamName(main.idOne);
		console.log(main.a);
		main.wins = main.a[0];
		main.loses = main.a[1];
		main.draws = main.a[2];
		main.gfs = main.a[3];
		main.gas = main.a[4];
		main.name = main.a[5];	

		}, function(error){
			console.log(error.statusText);
		});
		
	}

main.loadOne(main.idOne);

}]);


// Stats Controller 2
app.controller("statsTwoController", ['$http', '$routeParams', 'factoryService', function($http,$routeParams, factoryService){

	var main = this;
	main.data = [];
	console.log(main.data.length);

	this.idTwo = $routeParams.teamNameTwo;
	console.log(typeof(this.idOne));

	if(this.idTwo === null || this.idTwo === undefined){
		this.matchTwo = false;
	}
	else{
		this.matchTwo = true;
	}

	
	// WIN SCORES
	this.ARSwinOne =0;
	this.BURwinOne =0;
	this.BOUwinOne =0;
	this.CHEwinOne =0;
	this.CRYwinOne =0;
	this.EVEwinOne =0;
	this.HULwinOne =0;
	this.LEIwinOne =0;
	this.LIVwinOne =0;
	this.MCIwinOne =0;
	this.MUNwinOne =0;
	this.MFCwinOne =0;
	this.SOUwinOne =0;
	this.STKwinOne =0;
	this.SUNwinOne =0;
	this.SWAwinOne =0;
	this.TOTwinOne =0;
	this.WATwinOne =0;
	this.WBAwinOne =0;
	this.WHUwinOne =0;

	// DRAW SCORES
	this.ARSdrawOne = 0;
	this.BURdrawOne = 0;
	this.BOUdrawOne = 0;
	this.CHEdrawOne = 0;
	this.CRYdrawOne = 0;
	this.EVEdrawOne = 0;
	this.HULdrawOne = 0;
	this.LEIdrawOne = 0;
	this.LIVdrawOne = 0;
	this.MCIdrawOne = 0;
	this.MUNdrawOne = 0;
	this.MFCdrawOne = 0;
	this.SOUdrawOne = 0;
	this.STKdrawOne = 0;
	this.SUNdrawOne = 0;
	this.SWAdrawOne = 0;
	this.TOTdrawOne = 0;
	this.WATdrawOne = 0;
	this.WBAdrawOne = 0;
	this.WHUdrawOne = 0;

	// LOSE MATCHES
	this.ARSloseOne = 0;
	this.BURloseOne = 0;
	this.BOUloseOne = 0;
	this.CHEloseOne = 0;
	this.CRYloseOne = 0;
	this.EVEloseOne = 0;
	this.HULloseOne = 0;
	this.LEIloseOne = 0;
	this.LIVloseOne = 0;
	this.MCIloseOne = 0;
	this.MUNloseOne = 0;
	this.MFCloseOne = 0;
	this.SOUloseOne = 0;
	this.STKloseOne = 0;
	this.SUNloseOne = 0;
	this.SWAloseOne = 0;
	this.TOTloseOne = 0;
	this.WATloseOne = 0;
	this.WBAloseOne = 0;
	this.WHUloseOne = 0;

	// VARS to find gf and ga
	this.scoreARSOne = 0;
	this.onARSOne = 0;

	this.scoreBUROne = 0;
	this.onBUROne = 0;

	this.scoreBOUOne = 0;
	this.onBOUOne = 0;

	this.scoreCHEOne = 0;
	this.onCHEOne = 0;

	this.scoreCRYOne = 0;
	this.onCRYOne = 0;

	this.scoreEVEOne = 0;
	this.onEVEOne = 0;

	this.scoreHULOne = 0;
	this.onHULOne = 0;

	this.scoreLEIOne = 0;
	this.onLEIOne = 0;

	this.scoreLIVOne = 0;
	this.onLIVOne = 0;

	this.scoreMCIOne = 0;
	this.onMCIOne = 0;

	this.scoreMUNOne = 0;
	this.onMUNOne = 0;

	this.scoreMFCOne = 0;
	this.onMFCOne = 0;

	this.scoreSOUOne = 0;
	this.onSOUOne = 0;

	this.scoreSTKOne = 0;
	this.onSTKOne = 0;

	this.scoreSUNOne = 0;
	this.onSUNOne = 0;

	this.scoreSWAOne = 0;
	this.onSWAOne = 0;

	this.scoreTOTOne = 0;
	this.onTOTOne = 0;

	this.scoreWATOne = 0;
	this.onWATOne = 0;

	this.scoreWBAOne = 0;
	this.onWBAOne = 0;

	this.scoreWHUOne = 0;
	this.onWHUOne = 0;
	
	
	// 2016-17 data
	main.loadOne = function(id){
		
		factoryService.getDataTwo()
		.then(function (result){
			// promise was fullfilled (regardless of outcome)
			// checks for information will be performed here
			main.dataOne = result;
			console.log(main.dataOne);
			
			main.teamName = function(value){
				switch (value){
					case 'ARS':
						for(var i = 0; i < main.dataOne.length; i++){	
							for(var j = 0; j < main.dataOne[i].matches.length; j++){
								// TEAM ARS
								if(main.dataOne[i].matches[j].team1.code === "ARS"){
									main.nameOne = main.dataOne[i].matches[j].team1.name;
									main.scoreARSOne += main.dataOne[i].matches[j].score1;
									
									main.onARSOne += main.dataOne[i].matches[j].score2;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										main.ARSwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.ARSloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.ARSdrawOne++;
										
									}
								}
								else if(main.dataOne[i].matches[j].team2.code === "ARS"){
									main.scoreARSOne += main.dataOne[i].matches[j].score2;

									main.onARSOne += main.dataOne[i].matches[j].score1;
									
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										
										main.ARSloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.ARSwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.ARSdrawOne++;
										
									}
								}	
							}
						}
						return [main.ARSwinOne, main.ARSloseOne, main.ARSdrawOne, main.scoreARSOne, main.onARSOne, main.nameOne];
						break;

					case 'BUR':
						for(var i = 0; i < main.dataOne.length; i++){
							console.log(main.dataOne[i].name);
							for(var j = 0; j < main.dataOne[i].matches.length; j++){
								// TEAM BUR
								if(main.dataOne[i].matches[j].team1.code === "BUR"){
									main.nameOne = main.dataOne[i].matches[j].team1.name;
									main.scoreBUROne += main.dataOne[i].matches[j].score1;
									
									main.onBUROne += main.dataOne[i].matches[j].score2;	
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										main.BURwinOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.BURloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.BURdrawOne++;
										
									}	
								}
								else if(main.dataOne[i].matches[j].team2.code === "BUR"){
									main.scoreBUROne += main.dataOne[i].matches[j].score2;

									main.onBUROne += main.dataOne[i].matches[j].score1;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										
										main.BURloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.BURwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 === null && main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.BURdrawOne++;
										
									}
								}
							}
						}
						return [main.BURwinOne, main.BURloseOne, main.BURdrawOne, main.scoreBUROne, main.onBUROne, main.nameOne];
						break;
						

					case 'BOU':
						for(var i = 0; i < main.dataOne.length; i++){
							
							for(var j = 0; j < main.dataOne[i].matches.length; j++){
								// TEAM BOU
								if(main.dataOne[i].matches[j].team1.code === "BOU"){
									main.nameOne = main.dataOne[i].matches[j].team1.name;
									main.scoreBOUOne += main.dataOne[i].matches[j].score1;
									
									main.onBOUOne += main.dataOne[i].matches[j].score2;	
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										main.BOUwinOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.BOUloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.BOUdrawOne++;
										
									}	
								}
								else if(main.dataOne[i].matches[j].team2.code === "BOU"){
									main.scoreBOUOne += main.dataOne[i].matches[j].score2;

									main.onBOUOne += main.dataOne[i].matches[j].score1;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										
										main.BOUloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.BOUwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 === null && main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.BOUdrawOne++;
										
									}
								}
							}
						}
						return [main.BOUwinOne, main.BOUloseOne, main.BOUdrawOne, main.scoreBOUOne, main.onBOUOne, main.nameOne];
						break;
						

					case 'CHE':
						for(var i = 0; i < main.dataOne.length; i++){
							
							for(var j = 0; j < main.dataOne[i].matches.length; j++){
								// TEAM CHE
								if(main.dataOne[i].matches[j].team1.code === "CHE"){
									main.nameOne = main.dataOne[i].matches[j].team1.name;
									main.scoreCHEOne += main.dataOne[i].matches[j].score1;
									
									main.onCHEOne += main.dataOne[i].matches[j].score2;	
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										main.CHEwinOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.CHEloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.CHEdrawOne++;
										
									}	
								}
								else if(main.dataOne[i].matches[j].team2.code === "CHE"){
									main.scoreCHEOne += main.dataOne[i].matches[j].score2;

									main.onCHEOne += main.dataOne[i].matches[j].score1;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										
										main.CHEloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.CHEwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 === null && main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.CHEdrawOne++;
										
									}
								}
							}
						}
						return [main.CHEwinOne, main.CHEloseOne, main.CHEdrawOne, main.scoreCHEOne, main.onCHEOne, main.nameOne];
						break;
						

					case 'CRY':
						for(var i = 0; i < main.dataOne.length; i++){
							
							for(var j = 0; j < main.dataOne[i].matches.length; j++){
								// TEAM CRY
								if(main.dataOne[i].matches[j].team1.code === "CRY"){
									main.nameOne = main.dataOne[i].matches[j].team1.name;	
									main.scoreCRYOne += main.dataOne[i].matches[j].score1;
									
									main.onCRYOne += main.dataOne[i].matches[j].score2;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										main.CRYwinOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.CRYloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.CRYdrawOne++;
										
									}	
								}
								else if(main.dataOne[i].matches[j].team2.code === "CRY"){
									main.scoreCRYOne += main.dataOne[i].matches[j].score2;

									main.onCRYOne += main.dataOne[i].matches[j].score1;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										
										main.CRYloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.CRYwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 === null && main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.CRYdrawOne++;
										
									}
								}
							}
						}
						return [main.CRYwinOne, main.CRYloseOne, main.CRYdrawOne, main.scoreCRYOne, main.onCRYOne, main.nameOne];
						break;
						

					case 'EVE':
						for(var i = 0; i < main.dataOne.length; i++){
							
							for(var j = 0; j < main.dataOne[i].matches.length; j++){
								// TEAM EVE
								if(main.dataOne[i].matches[j].team1.code === "EVE"){
									main.nameOne = main.dataOne[i].matches[j].team1.name;	
									main.scoreEVEOne += main.dataOne[i].matches[j].score1;
									
									main.onEVEOne += main.dataOne[i].matches[j].score2;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										main.EVEwinOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.EVEloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.EVEdrawOne++;
										
									}	
								}
								else if(main.dataOne[i].matches[j].team2.code === "EVE"){
									main.scoreEVEOne += main.dataOne[i].matches[j].score2;

									main.onEVEOne += main.dataOne[i].matches[j].score1;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										
										main.EVEloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.EVEwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 === null && main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.EVEdrawOne++;
										
									}
								}
							}
						}
						return [main.EVEwinOne, main.EVEloseOne, main.EVEdrawOne, main.scoreEVEOne, main.onEVEOne, main.nameOne];
						break;
						
					case 'HUL':
						for(var i = 0; i < main.dataOne.length; i++){
							
							for(var j = 0; j < main.dataOne[i].matches.length; j++){
								// TEAM NEW
								if(main.dataOne[i].matches[j].team1.code === "HUL"){	
									main.nameOne = main.dataOne[i].matches[j].team1.name;
									main.scoreHULOne += main.dataOne[i].matches[j].score1;
									
									main.onHULOne += main.dataOne[i].matches[j].score2;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										main.HULwinOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.HULloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.HULdrawOne++;
										
									}	
								}
								else if(main.dataOne[i].matches[j].team2.code === "HUL"){
									main.scoreHULOne += main.dataOne[i].matches[j].score2;

									main.onHULOne += main.dataOne[i].matches[j].score1;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										
										main.HULloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.HULwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 === null && main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.HULdrawOne++;
										
									}
								}
							}
						}
						return [main.HULwinOne, main.HULloseOne, main.HULdrawOne, main.scoreHULOne, main.onHULOne, main.nameOne];
						break;


					case 'LEI':
						for(var i = 0; i < main.dataOne.length; i++){
							
							for(var j = 0; j < main.dataOne[i].matches.length; j++){
								// TEAM LEI
								if(main.dataOne[i].matches[j].team1.code === "LEI"){
									main.nameOne = main.dataOne[i].matches[j].team1.name;	
									main.scoreLEIOne += main.dataOne[i].matches[j].score1;
									
									main.onLEIOne += main.dataOne[i].matches[j].score2;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										main.LEIwinOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.LEIloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.LEIdrawOne++;
										
									}	
								}
								else if(main.dataOne[i].matches[j].team2.code === "LEI"){
									main.scoreLEIOne += main.dataOne[i].matches[j].score2;

									main.onLEIOne += main.dataOne[i].matches[j].score1;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										
										main.LEIloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.LEIwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 === null && main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.LEIdrawOne++;
										
									}
								}
							}
						}
						return [main.LEIwinOne, main.LEIloseOne, main.LEIdrawOne, main.scoreLEIOne, main.onLEIOne, main.nameOne];
						break;
						

					case 'LIV':
						for(var i = 0; i < main.dataOne.length; i++){
							
							for(var j = 0; j < main.dataOne[i].matches.length; j++){
								// TEAM LIV
								if(main.dataOne[i].matches[j].team1.code === "LIV"){	
									main.nameOne = main.dataOne[i].matches[j].team1.name;
									main.scoreLIVOne += main.dataOne[i].matches[j].score1;
									
									main.onLIVOne += main.dataOne[i].matches[j].score2;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										main.LIVwinOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.LIVloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.LIVdrawOne++;
										
									}	
								}
								else if(main.dataOne[i].matches[j].team2.code === "LIV"){
									main.scoreLIVOne += main.dataOne[i].matches[j].score2;

									main.onLIVOne += main.dataOne[i].matches[j].score1;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										
										main.LIVloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.LIVwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 === null && main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.LIVdrawOne++;
										
									}
								}
							}
						}
						return [main.LIVwinOne, main.LIVloseOne, main.LIVdrawOne, main.scoreLIVOne, main.onLIVOne, main.nameOne];
						break;
						

					case 'MCI':
						for(var i = 0; i < main.dataOne.length; i++){
							
							for(var j = 0; j < main.dataOne[i].matches.length; j++){
								// TEAM MCI
								if(main.dataOne[i].matches[j].team1.code === "MCI"){	
									main.nameOne = main.dataOne[i].matches[j].team1.name;
									main.scoreMCIOne += main.dataOne[i].matches[j].score1;
									
									main.onMCIOne += main.dataOne[i].matches[j].score2;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										main.MCIwinOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.MCIloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.MCIdrawOne++;
										
									}	
								}
								else if(main.dataOne[i].matches[j].team2.code === "MCI"){
									main.scoreMCIOne += main.dataOne[i].matches[j].score2;

									main.onMCIOne += main.dataOne[i].matches[j].score1;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										
										main.MCIloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.MCIwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 === null && main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.MCIdrawOne++;
										
									}
								}
							}
						}
						return [main.MCIwinOne, main.MCIloseOne, main.MCIdrawOne, main.scoreMCIOne, main.onMCIOne, main.nameOne];
						break;
						

					case 'MUN':
						for(var i = 0; i < main.dataOne.length; i++){
							
							for(var j = 0; j < main.dataOne[i].matches.length; j++){
								// TEAM MUN
								if(main.dataOne[i].matches[j].team1.code === "MUN"){	
									main.nameOne = main.dataOne[i].matches[j].team1.name;
									main.scoreMUNOne += main.dataOne[i].matches[j].score1;
									
									main.onMUNOne += main.dataOne[i].matches[j].score2;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										main.MUNwinOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.MUNloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.MUNdrawOne++;
										
									}	
								}
								else if(main.dataOne[i].matches[j].team2.code === "MUN"){
									main.scoreMUNOne += main.dataOne[i].matches[j].score2;

									main.onMUNOne += main.dataOne[i].matches[j].score1;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										
										main.MUNloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.MUNwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 === null && main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.MUNdrawOne++;
										
									}
								}
							}
						}
						return [main.MUNwinOne, main.MUNloseOne, main.MUNdrawOne, main.scoreMUNOne, main.onMUNOne, main.nameOne];
						break;
						

					
						

					case 'MFC':
						for(var i = 0; i < main.dataOne.length; i++){
							
							for(var j = 0; j < main.dataOne[i].matches.length; j++){
								// TEAM NOR
								if(main.dataOne[i].matches[j].team1.code === "MFC"){	
									main.nameOne = main.dataOne[i].matches[j].team1.name;
									main.scoreMFCOne += main.dataOne[i].matches[j].score1;
									
									main.onMFCOne += main.dataOne[i].matches[j].score2;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										main.MFCwinOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.MFCloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.MFCdrawOne++;
										
									}	
								}
								else if(main.dataOne[i].matches[j].team2.code === "MFC"){
									main.scoreMFCOne += main.dataOne[i].matches[j].score2;

									main.onMFCOne += main.dataOne[i].matches[j].score1;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										
										main.MFCloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.MFCwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 === null && main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.MFCdrawOne++;
										
									}
								}
							}
						}
						return [main.MFCwinOne, main.MFCloseOne, main.MFCdrawOne, main.scoreMFCOne, main.onMFCOne, main.nameOne];
						break;
						

					case 'SOU':
						for(var i = 0; i < main.dataOne.length; i++){
							
							for(var j = 0; j < main.dataOne[i].matches.length; j++){
								// TEAM SOU
								if(main.dataOne[i].matches[j].team1.code === "SOU"){	
									main.nameOne = main.dataOne[i].matches[j].team1.name;
									main.scoreSOUOne += main.dataOne[i].matches[j].score1;
									
									main.onSOUOne += main.dataOne[i].matches[j].score2;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										main.SOUwinOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.SOUloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.SOUdrawOne++;
										
									}	
								}
								else if(main.dataOne[i].matches[j].team2.code === "SOU"){
									main.scoreSOUOne += main.dataOne[i].matches[j].score2;

									main.onSOUOne += main.dataOne[i].matches[j].score1;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										
										main.SOUloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.SOUwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 === null && main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.SOUdrawOne++;
										
									}
								}
							}
						}
						return [main.SOUwinOne, main.SOUloseOne, main.SOUdrawOne, main.scoreSOUOne, main.onSOUOne, main.nameOne];
						break;
						

					case 'STK':
						for(var i = 0; i < main.dataOne.length; i++){
							
							for(var j = 0; j < main.dataOne[i].matches.length; j++){
								// TEAM STK
								if(main.dataOne[i].matches[j].team1.code === "STK"){	
									main.nameOne = main.dataOne[i].matches[j].team1.name;
									main.scoreSTKOne += main.dataOne[i].matches[j].score1;
									
									main.onSTKOne += main.dataOne[i].matches[j].score2;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										main.STKwinOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.STKloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.STKdrawOne++;
										
									}	
								}
								else if(main.dataOne[i].matches[j].team2.code === "STK"){
									main.scoreSTKOne += main.dataOne[i].matches[j].score2;

									main.onSTKOne += main.dataOne[i].matches[j].score1;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										
										main.STKloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.STKwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 === null && main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.STKdrawOne++;
										
									}
								}
							}
						}
						return [main.STKwinOne, main.STKloseOne, main.STKdrawOne, main.scoreSTKOne, main.onSTKOne, main.nameOne];
						break;
						

					case 'SUN':
						for(var i = 0; i < main.dataOne.length; i++){
							
							for(var j = 0; j < main.dataOne[i].matches.length; j++){
								// TEAM SUN
								if(main.dataOne[i].matches[j].team1.code === "SUN"){	
									main.nameOne = main.dataOne[i].matches[j].team1.name;
									main.scoreSUNOne += main.dataOne[i].matches[j].score1;
									
									main.onSUNOne += main.dataOne[i].matches[j].score2;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										main.SUNwinOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.SUNloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.SUNdrawOne++;
										
									}	
								}
								else if(main.dataOne[i].matches[j].team2.code === "SUN"){
									main.scoreSUNOne += main.dataOne[i].matches[j].score2;

									main.onSUNOne += main.dataOne[i].matches[j].score1;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										
										main.SUNloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.SUNwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 === null && main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.SUNdrawOne++;
										
									}
								}
							}
						}
						return [main.SUNwinOne, main.SUNloseOne, main.SUNdrawOne, main.scoreSUNOne, main.onSUNOne, main.nameOne];
						break;
						

					case 'SWA':
						for(var i = 0; i < main.dataOne.length; i++){
							
							for(var j = 0; j < main.dataOne[i].matches.length; j++){
								// TEAM SWA
								if(main.dataOne[i].matches[j].team1.code === "SWA"){	
									main.nameOne = main.dataOne[i].matches[j].team1.name;
									main.scoreSWAOne += main.dataOne[i].matches[j].score1;
									
									main.onSWAOne += main.dataOne[i].matches[j].score2;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										main.SWAwinOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.SWAloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.SWAdrawOne++;
										
									}	
								}
								else if(main.dataOne[i].matches[j].team2.code === "SWA"){
									main.scoreSWAOne += main.dataOne[i].matches[j].score2;

									main.onSWAOne += main.dataOne[i].matches[j].score1;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										
										main.SWAloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.SWAwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 === null && main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.SWAdrawOne++;
										
									}
								}
							}
						}
						return [main.SWAwinOne, main.SWAloseOne, main.SWAdrawOne, main.scoreSWAOne, main.onSWAOne, main.nameOne];
						break;
						

					case 'TOT':
						for(var i = 0; i < main.dataOne.length; i++){
							
							for(var j = 0; j < main.dataOne[i].matches.length; j++){
								// TEAM TOT
								if(main.dataOne[i].matches[j].team1.code === "TOT"){	
									main.nameOne = main.dataOne[i].matches[j].team1.name;
									main.scoreTOTOne += main.dataOne[i].matches[j].score1;
									
									main.onTOTOne += main.dataOne[i].matches[j].score2;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										main.TOTwinOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.TOTloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.TOTdrawOne++;
										
									}	
								}
								else if(main.dataOne[i].matches[j].team2.code === "TOT"){
									main.scoreTOTOne += main.dataOne[i].matches[j].score2;

									main.onTOTOne += main.dataOne[i].matches[j].score1;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										
										main.TOTloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.TOTwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 === null && main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.TOTdrawOne++;
										
									}
								}
							}
						}
						return [main.TOTwinOne, main.TOTloseOne, main.TOTdrawOne, main.scoreTOTOne, main.onTOTOne, main.nameOne];
						break;
						

					case 'WAT':
						for(var i = 0; i < main.dataOne.length; i++){
							
							for(var j = 0; j < main.dataOne[i].matches.length; j++){
								// TEAM WAT
								if(main.dataOne[i].matches[j].team1.code === "WAT"){	
									main.nameOne = main.dataOne[i].matches[j].team1.name;
									main.scoreWATOne += main.dataOne[i].matches[j].score1;
									
									main.onWATOne += main.dataOne[i].matches[j].score2;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										main.WATwinOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.WATloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.WATdrawOne++;
									
									}	
								}
								else if(main.dataOne[i].matches[j].team2.code === "WAT"){
									main.scoreWATOne += main.dataOne[i].matches[j].score2;

									main.onWATOne += main.dataOne[i].matches[j].score1;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										
										main.WATloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.WATwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 === null && main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.WATdrawOne++;
										
									}
								}
							}
						}
						return [main.WATwinOne, main.WATloseOne, main.WATdrawOne, main.scoreWATOne, main.onWATOne, main.nameOne];
						break;
						

					case 'WBA':
						for(var i = 0; i < main.dataOne.length; i++){
							
							for(var j = 0; j < main.dataOne[i].matches.length; j++){
								// TEAM WBA
								if(main.dataOne[i].matches[j].team1.code === "WBA"){	
									main.nameOne = main.dataOne[i].matches[j].team1.name;
									main.scoreWBAOne += main.dataOne[i].matches[j].score1;
									
									main.onWBAOne += main.dataOne[i].matches[j].score2;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										main.WBAwinOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										main.WBAloseOne++;
										
										
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.WBAdrawOne++;
										
									}	
								}
								else if(main.dataOne[i].matches[j].team2.code === "WBA"){
									main.scoreWBAOne += main.dataOne[i].matches[j].score2;

									main.onWBAOne += main.dataOne[i].matches[j].score1;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										
										main.WBAloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.WBAwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 === null && main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.WBAdrawOne++;
										
									}
								}
							}
						}
						return [main.WBAwinOne, main.WBAloseOne, main.WBAdrawOne, main.scoreWBAOne, main.onWBAOne, main.nameOne];
						break;
						

					case 'WHU':
						for(var i = 0; i < main.dataOne.length; i++){
							
							for(var j = 0; j < main.dataOne[i].matches.length; j++){
								// TEAM WHU
								if(main.dataOne[i].matches[j].team1.code === "WHU"){	
									main.nameOne = main.dataOne[i].matches[j].team1.name;
									main.scoreWHUOne += main.dataOne[i].matches[j].score1;
									
									main.onWHUOne += main.dataOne[i].matches[j].score2;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										main.WHUwinOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										main.WHUloseOne++;
										
										
									}
									else if(main.dataOne[i].matches[j].score1 === null || main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled")
									}
									else{
										
										main.WHUdrawOne++;
										
									}	
								}
								else if(main.dataOne[i].matches[j].team2.code === "WHU"){
									main.scoreWHUOne += main.dataOne[i].matches[j].score2;

									main.onWHUOne += main.dataOne[i].matches[j].score1;
									if(main.dataOne[i].matches[j].score1 > main.dataOne[i].matches[j].score2){
										
										main.WHUloseOne++;
										
									}
									else if(main.dataOne[i].matches[j].score1 < main.dataOne[i].matches[j].score2){
										
										main.WHUwinOne++;
									}
									else if(main.dataOne[i].matches[j].score1 === null && main.dataOne[i].matches[j].score2 === null){
										console.log("Match Cancelled");
									}
									else{
										
										main.WHUdrawOne++;
										
									}
								}
							}
						}
						return [main.WHUwinOne, main.WHUloseOne, main.WHUdrawOne, main.scoreWHUOne, main.onWHUOne, main.nameOne];
						break;	
						
				}
			}

		main.a = main.teamName(main.idTwo);
		console.log(main.a);
		main.wins = main.a[0];
		main.loses = main.a[1];
		main.draws = main.a[2];
		main.gfs = main.a[3];
		main.gas = main.a[4];
		main.name = main.a[5];
			

		}, function(error){
			console.log(error.statusText);
		});
		
	}

main.loadOne(main.idTwo);

}]);