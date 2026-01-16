function addKeywords(keywords, type, language = 'cpp')
{
	var existingKeywords = hljs.getLanguage(language).keywords;
	keywords.forEach(function(value){ existingKeywords[type].push(value) });
}

function addCustomFns(fns, language = 'cpp')
{
	addKeywords(fns, 'built_in', language);
}

function addCustomTypes(types, language = 'cpp')
{
	addKeywords(types, 'type', language);
}

function addCustomNames(names, language = 'cpp')
{
	addKeywords(names, 'keyword', language);
}

function hljs_moveTypesAndKeywordsToLiterals(language = 'cpp')
{
	var existingTypes = hljs.getLanguage(language).keywords.type;
	var existingKeywords = hljs.getLanguage(language).keywords.keyword;
	var existingLiterals = hljs.getLanguage(language).keywords.literal;
	existingTypes.forEach(function(value){ existingLiterals.push(value); });
	existingKeywords.forEach(function(value){ existingLiterals.push(value); });
	existingTypes = [];
	existingKeywords = [];
}