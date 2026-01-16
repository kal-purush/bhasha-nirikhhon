'use strict';

class ModalVideoComponent {
	/*@ngInject*/
	constructor($scope, $element, $state, $stateParams,  elementType){
		this.$scope = $scope;
		this.$element = $element;
		this.$state = $state;
		this.$stateParams = $stateParams;
		//this.Restangular = Restangular;
	//	this.Resource = this.Restangular.one(elementType, $scope.$parent.resource._id);

		this.loading = true;
	}


	
}


export default angular.module('robotica.social.components.modalVideo', [])
											.component('modalView', {
												template: require('./modalVideo.html'),
												controller: ModalVideoComponent,
												controllerAs: '$ctrl'
											})
											.name;