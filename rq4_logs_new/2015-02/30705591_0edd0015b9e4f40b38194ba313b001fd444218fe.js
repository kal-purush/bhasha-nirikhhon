'use strict';

angular.module('organizations')
	.directive('autoCompleteOwner', ['$http', function($http) {
		return {
			restrict: 'AE',
			scope:{
				selectedTags:'=model'
			},
			templateUrl:'modules/organizations/views/auto-complete-owner.template.html',
			link: function postLink(scope, element, attrs) {

				scope.editMode = false;

				scope.suggestions=[];

				//scope.selectedTags=[];

				scope.selectedIndex=-1;

				scope.removeTag=function(index){
					//scope.selectedTags.splice(index,1);
					scope.selectedTags == null;
					scope.editMode = false;
				}

				scope.search=function(){
					$http.get(attrs.url+'/'+scope.searchText.firstName).success(function(data){
						/*if(data.indexOf(scope.searchText)===-1){
						 data.unshift(scope.searchText.firstName);
						 }*/
						scope.suggestions=data;
						scope.selectedIndex=-1;
					});
				}

				scope.addToSelectedTags=function(index){
					/*if(scope.selectedTags.indexOf(scope.suggestions[index])===-1){

						/*for(var i = 0; i < scope.suggestions.length; i++) {
						 if (scope.selectedTags[i] != scope.suggestions[index]) {
						 scope.selectedTags.push(scope.suggestions[index]);
						 }
						 }*/
						/*scope.editMode = true;
						scope.selectedTags.push(scope.suggestions[index]);
						scope.searchText='';
						scope.suggestions=[];
					}*/
					scope.selectedTags = scope.suggestions[index];
					scope.searchText='';
					scope.suggestions=[];
				}

				scope.checkKeyDown=function(event){
					if(event.keyCode===40){
						event.preventDefault();
						if(scope.selectedIndex+1 !== scope.suggestions.length){
							scope.selectedIndex++;
						}
					}
					else if(event.keyCode===38){
						event.preventDefault();
						if(scope.selectedIndex-1 !== -1){
							scope.selectedIndex--;
						}
					}
					else if(event.keyCode===13){
						scope.addToSelectedTags(scope.selectedIndex);
					}
				}

				scope.$watch('selectedIndex',function(val){

					if(val!==-1) {
						scope.searchText = scope.suggestions[scope.selectedIndex];
					}
				});
				//element.text('this is the autoComplete directive');
			}
		};
	}
]);