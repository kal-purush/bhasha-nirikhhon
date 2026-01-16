/// <reference path="../../../.meteor/local/build/programs/server/assets/packages/meteortypescript_typescript-libs/definitions/all-definitions.d.ts" />
/// <reference path="../../../definitions/materialize.d.ts" />
/// <reference path="../../../lib/constants.ts" />

Template["Settings"].onRendered(function() {
	$('.collapsible').collapsible();
});

Template["Settings"].helpers({
	getLanguages: function() {
		var user = Meteor.user();
		var outputString: string = '';
		var leftColumn: string = '<div class="col m4 s12">';
		var rightColumn: string = '<div class="col m4 s12">';
		var middleColumn: string = '<div class="col m4 s12">';
		
		var perColumn: number = Math.floor(Languages.length / 3);
		
		outputString += '<div class="row">';
		_.each(Languages, function(language: string, index: number) {
			if (index < 33) {
				leftColumn += `<input type="checkbox" name="${language}" />`;
				leftColumn += `<label for="${language}">${language}</label><br>`;
			} else if (index >= 33 && index < 66) {
				middleColumn += `<input type="checkbox" name="${language}" />`;
				middleColumn += `<label for="${language}">${language}</label><br>`;
			} else {
				rightColumn += `<input type="checkbox" name="${language}" />`;
				rightColumn += `<label for="${language}">${language}</label><br>`;
			}
		});
		
		leftColumn += '</div>';
		middleColumn += '</div>';
		rightColumn += '</div>';
		
		outputString += `${leftColumn}${middleColumn}${rightColumn}</div>`;
		return outputString;
	}
});