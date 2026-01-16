cuff_ui.controller('contributorController', ['$scope', 'SubmissionService', 'DropdownService', '$location', function($scope, SubmissionService, DropdownService, $location) {
	$scope.entry_type='New';
	$scope.first_name;
	$scope.last_name;
	$scope.city;
	$scope.state;
	$scope.zipcode;
	$scope.states = DropdownService.getStateList();
	$scope.relationships = DropdownService.getRelationshipList();
	$scope.email_address;
	$scope.phone_number;
	$scope.preferred_contact_method='email';
	$scope.relationship;
	$scope.contact_name;
	$scope.contact_organization;
	$scope.contact_city;
	$scope.contact_state;
	$scope.contact_phone_number;
	$scope.contact_email_address;
	
	$scope.nextStep = function(isValid) {
			console.log('Validating form. Valid? '+isValid)
			if (isValid) {
				SubmissionService.setContributor($scope);
				$location.path('subject')
			}
		}

	if (!_.isEmpty(SubmissionService.getContributor())) {
		console.log('SubmissionService contains Contribuor information: ')
		var c = SubmissionService.getContributor()
		console.log(c)
		$scope.entry_type = c.entry_type
		$scope.first_name = c.first_name
		$scope.last_name = c.last_name
		$scope.city = c.city;
		$scope.state = c.state;
		$scope.zipcode = c.zipcode;
		$scope.email_address = c.email_address;
		$scope.phone_number = c.phone_number;
		$scope.preferred_contact_method = c.preferred_contact_method;
		$scope.relationship = c.relationship;
		$scope.contact_name = c.contact_name;
		$scope.contact_organization = c.contact_organization;
		$scope.contact_city = c.contact_city;
		$scope.contact_state = c.contact_state;
		$scope.contact_phone_number = c.contact_phone_number;
		$scope.contact_email_address = c.contact_email_address;
	}


	if ($location.host()=='localhost') {
		$scope.first_name = 'John';
		$scope.last_name = 'Smith';
		$scope.city = 'City';
		$scope.state = 'AL';
		$scope.zipcode = 51423;
		$scope.email_address = 'me@xyz.com';
		$scope.phone_number = '670-394-1231';
		$scope.preferred_contact_method = 'email';
		$scope.relationship = 'Attorney';
		
		$scope.contact_name = 'Contact name';
		$scope.contact_organization = 'Contact organization';
		$scope.contact_city = 'Contact city';
		$scope.contact_state = 'AK';
		$scope.contact_phone_number = '123 456 7890';
		$scope.contact_email_address = 'abc@xyz.com';
	}
}]);