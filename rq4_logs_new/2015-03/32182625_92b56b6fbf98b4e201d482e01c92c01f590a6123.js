var args = arguments[0] || {};
var articleId = args.articleId || false;
var article = args.article || false;

var data_css =  "<head><style>" +
				"h1 {fontSize:14; fontWheight:bold; font:red}" +
				"</style></head>";

$.article.text = article;

var db = Ti.Database.open('_alloy_');

var article_what_is_RS = db.execute('SELECT body FROM article_what_is WHERE article_id = ' + articleId);
var article_what_is_body = article_what_is_RS.fieldByName('body');
$.whatis.html = data_css + article_what_is_body;

var article_symptoms_RS = db.execute('SELECT body FROM article_symptoms WHERE article_id = ' + articleId);
var article_symptoms_body = article_symptoms_RS.fieldByName('body');
$.symptoms.html = article_symptoms_body;

var article_diagnosis_RS = db.execute('SELECT body FROM article_diagnosis WHERE article_id = ' + articleId);
var article_diagnosis_body = article_diagnosis_RS.fieldByName('body');
$.diagnosis.html = article_diagnosis_body;

var article_duration_RS = db.execute('SELECT body FROM article_duration WHERE article_id = ' + articleId);
var article_duration_body = article_duration_RS.fieldByName('body');
$.duration.html = article_duration_body;

var article_prevention_RS = db.execute('SELECT body FROM article_prevention WHERE article_id = ' + articleId);
var article_prevention_body = article_prevention_RS.fieldByName('body');
$.prevention.html = article_prevention_body;

var article_treatment_RS = db.execute('SELECT body FROM article_treatment WHERE article_id = ' + articleId);
var article_treatment_body = article_treatment_RS.fieldByName('body');
$.treatment.html = article_treatment_body;

var article_when_contact_professional_RS = db.execute('SELECT body FROM article_when_contact_professional WHERE article_id = ' + articleId);
var article_when_contact_professional_body = article_when_contact_professional_RS.fieldByName('body');
$.when_contact_professional.html = article_when_contact_professional_body;

var article_prognosis_RS = db.execute('SELECT body FROM article_prognosis WHERE article_id = ' + articleId);
var article_prognosis_body = article_prognosis_RS.fieldByName('body');
$.prognosis.html = article_prognosis_body;

var article_aditional_info_RS = db.execute('SELECT body FROM article_aditional_info WHERE article_id = ' + articleId);
var article_aditional_info_body = article_aditional_info_RS.fieldByName('body');
$.aditional_info.html = article_aditional_info_body;

article_what_is_RS.close();

// var article_!_RS = db.execute('SELECT body FROM article_! WHERE article_id = ' + articleId);
// var article_!_body = article_!_RS.fieldByName('body');
// $.!.html = article_!_body;