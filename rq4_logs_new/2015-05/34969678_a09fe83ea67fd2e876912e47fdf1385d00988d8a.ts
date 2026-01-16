/// <reference path="../../.meteor/local/build/programs/server/assets/packages/meteortypescript_typescript-libs/definitions/all-definitions.d.ts" />
/// <reference path="../../lib/constants.ts" />

declare var SiteName : string;
SiteName =  'LinguaBit';

Handlebars.registerHelper('SiteName', () => {
	return SiteName;
});

Handlebars.registerHelper('PostLength', () => {
	return PostLength;
});

Handlebars.registerHelper('activeRoute', (..._arguments) => {
	var args = Array.prototype.slice.call(_arguments, 0);
	args.pop();
	
	var active : boolean = _.any(args, name => {
		return Router.current() && Router.current().route.getName() === name; 
	});
	
	return active && 'active';
});

Handlebars.registerHelper('languages', () => {
	return ["Afrikaans", "Ainu", "Albanian", "Arabic", "Armenian", "Azerbaijanian", "Basque", "Bengali", "Bihari", "Bosnian", "Breton", "Bulgarian", "Burmese", "Cantonese", "Catalan", "Croatian", "Czech", "Danish", "Dutch", "English", "Esperanto", "Estonian", "Faroese", "Finnish", "Flemish", "French", "Gaelic", "Georgian", "German", "Greek", "Haitian", "Hawaiian", "Hebrew", "Hindi", "Hungarian", "Icelandic", "Indonesian", "Irish", "Italian", "Japanese", "Javanese", "Kannada", "Khmer", "Kirghiz", "Korean", "Kurdish", "Laotian", "Latin", "Latvian", "Lithuanian", "Lojban", "Lojban", "Macedonian", "Malay", "Maltese", "Mandarin", "Marathi", "Mongolian", "Myanmar", "Navajo", "Norwegian", "Ossetic", "Other language", "Panjabi", "Pashto", "Persian", "Polish", "Portuguese(Brazil)", "Portuguese(Portugal)", "Romani", "Romanian", "Russian", "Sanskrit", "Serbian", "Sinhalese", "Slavic", "Slovak", "Slovenian", "Spanish", "Swahili", "Swedish", "Tagalog", "Tamil", "Telugu", "Thai", "Tibetan", "Tongan", "Traditional Chinese", "Turkish", "Turkmen", "Udmurt", "Ukrainian", "Urdu", "Uyghur", "Uzbek", "Vietnamese", "Welsh", "Yiddish", "Zulu"];
});