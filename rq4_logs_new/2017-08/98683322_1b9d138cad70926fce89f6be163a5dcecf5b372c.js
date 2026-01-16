var dbClient = require("../db");
var https = require("https");
var cheerio = require("cheerio");
var querystring = require("querystring");
var moment = require("moment");

var DAY_TICKS = 1000*60*60*24;
var CLASSES_HTML_START = '<tr valign="top">';
var CLASSES_HTML_END = '</tr>';
var SCHEDULE_HTML_START = '<!--BEGIN content-->';
var SCHEDULE_HTML_END = '<!--END content-->';
var USER_INFO_HTML_START = 'action="/classes/student_info.php"';
var USER_INFO_HTML_END = '</form>';
var REGISTRATION_HTML_START = "<title>";
var REGISTRATION_HTML_END = "</title>";

exports.getClasses = function(desired_date, callback){
	// TODO: validate desired_date
	// TODO: Check it's in the future
	var postData = querystring.stringify({
		desired_date: desired_date
	});
	console.log("Get classes: ", desired_date);
	if(!moment(desired_date).isValid()) {
		callback([]);
		return;
	}
	
  	var options = {
	    host: 'washingtondc.trapezeschool.com',
	    port: 443,
	    path: '/classes/choose_class.php',
	    method: 'POST',
	    headers: {
	      'Content-Type': 'application/x-www-form-urlencoded',
	      'Content-Length': Buffer.byteLength(postData)
	    }
	  };
	
	var data = "";
  	var httpreq = https.request(options, function (response) {
	    response.setEncoding('utf8');
	    response.on('data', function (chunk) {
	    	data += chunk;
	    });
	    response.on('end', function() {
	      var dataRows = [];
	      var dataStart = data.indexOf(CLASSES_HTML_START);

	      if(dataStart > 1000){
		      var dataEnd = data.lastIndexOf(CLASSES_HTML_START);
		      var d = cheerio.load(data.substring(dataStart, data.indexOf(CLASSES_HTML_END, dataEnd) + CLASSES_HTML_END.length))
		      var trs = d('tr');
		      
		      trs.each(function(){
		      	var row = {};
		      	var tds = d(this).find('td');
		      	tds.each(function(i){
		      		var el = d(this);
		      		switch(i){
		      			case 0: // class_id
		      				var input = el.find('input');
		      				row["id"] = input.val() || "";
		      			break;
		      			case 1: // class_name
		      				row["name"] = el.text().replace(/[\s]+/g, " ");
		      				//var instructor = row["class_name"].match(/with[\s]([\w]+)/i);
		      				//row["instructor"] = (instructor && instructor.length) ? instructor[0].split(" ")[1] : "";
		      				break;
		      			case 2: // time
		      				var time = el.text().replace(/\s/g, "").split("-");
		      				row["start_time"] = time[0];
		      				row["end_time"] = time[1];
		      				row["start_date"] = desired_date;
		      				row["end_date"] = desired_date;
		      				break;
		      			case 3: // price
		      				row["cost"] = el.text().trim();
		      				break;
		      			case 4:
		      				row["availability"] = parseInt(el.text());
		      				row["description"] = "";
		      				row["status"] = (row["note"].match(/private/i) || row["note"].match(/cancelled/i) || row["note"].match(/full/i) || row["note"].match(/open/i) || [""])[0].toLowerCase()
		      				break;
		      		}
		      	});
		      	dataRows.push(row);
		      });
		      
	  		}

	  		callback(dataRows);
	  		
	    })
	  });
  	httpreq.write(postData);
  	httpreq.end();
}

exports.getUserInfo = function(email, DOB, callback){
	if(!DOB || typeof(DOB) !== "string") {
		callback({student_id: "", email: email, DOB: DOB});
		return;
	}

	var postData = querystring.stringify({
		login: "Student Log In",
		email: email,
		DOB_month: parseInt(DOB.split("-")[1]),
		DOB_day: parseInt(DOB.split("-")[2]),
		DOB_year: DOB.split("-")[0]
	});

	console.log(postData);

  	var options = {
	    host: 'washingtondc.trapezeschool.com',
	    port: 443,
	    path: '/classes/student_info.php',
	    method: 'POST',
	    headers: {
	      'Content-Type': 'application/x-www-form-urlencoded',
	      'Content-Length': Buffer.byteLength(postData)
	    }
	  };
	
	var data = "";
  	var httpreq = https.request(options, function (response) {
	    response.setEncoding('utf8');
	    response.on('data', function (chunk) {
	    	data += chunk;
	    });
	    response.on('end', function() {
	      var dataStart = data.indexOf(USER_INFO_HTML_START);
	      var info = {};
	      	if(dataStart > 1000){
				var d = cheerio.load(data.substring(dataStart, data.indexOf(USER_INFO_HTML_END, dataStart) + USER_INFO_HTML_END.length))

				var inputs = d('input[name][value],select[name]');
				
				inputs.each(function(){
					info[d(this).attr('name')] = d(this).val();
				});
	  		}
	  		if(!info.student_id) {
	  			callback({student_id: "", email: email, DOB: DOB});
	  		}else{
		  		callback({
		  			student_id: info.student_id,
		  			email: info.email,
		  			registration_date: info.registration_date,
		  			email2: info.email2,
		  			first_name: info.first_name,
		  			last_name: info.last_name,
		  			nickname: info.nickname,
		  			guardian: info.guardian,
		  			DOB_month: info.DOB_month,
		  			DOB_day: info.DOB_day,
		  			DOB_year: info.DOB_year,
		  			gender: info.gender,
		  			organization: info.organization,
		  			address1: info.address1,
		  			address2: info.address2,
		  			city: info.city,
		  			zip_code: info.zip_code,
		  			country: info.country,
		  			phone_home: info.phone_home,
		  			phone_mobile: info.phone_mobile,
		  			phone_work: info.phone_work,
		  			hear_about: info.hear_about,
		  			health_issues_btn: info.health_issues_btn,
		  			health_issues: info.health_issues,
		  			special_needs_btn: info.special_needs_btn,
		  			special_needs: info.special_needs,
		  			emergency_contact: info.emergency_contact,
		  			emergency_phone: info.emergency_phone,
		  			doctor_name: info.doctor_name,
		  			doctor_phone: info.doctor_phone,
		  			insurance: info.insurance
		  		});
	  		}
	    })
	  });
  	httpreq.write(postData);
  	httpreq.end();

}


exports.registerForClass = function(class_id, class_date, student_id, persons, payment_type, callback){

	var dataObject = {
		class_id: class_id, //279354 3/24/2017 = 282290
		date_time:class_date,
		student_id: student_id, //69236 
		persons:persons,
		payment_type:payment_type, // 5 is GC
		signup_by:"Student - app"
		//'gift_certificate[1]': ""
	};

	if(!student_id || !class_id || !payment_type || !persons) {
		callback({student_id: student_id, class_id: class_id, payment_type: payment_type, persons:persons});
		return;
	}

	var postData = querystring.stringify(dataObject);

  	var options = {
  		host: 'google.com',
	    //host: 'washingtondc.trapezeschool.com',
	    port: 443,
	    //path: '/classes/signup_complete.php',
	    method: 'POST',
	    headers: {
	      'Content-Type': 'application/x-www-form-urlencoded',
	      'Content-Length': Buffer.byteLength(querystring.stringify(dataObject))
	    }
	  };
	
	var data = "";
  	var httpreq = https.request(options, function (response) {
	    response.setEncoding('utf8');
	    response.on('data', function (chunk) {
	    	data += chunk;
	    });
	    response.on('end', function() {
	      	var dataStart = data.indexOf(REGISTRATION_HTML_START);
	      	var info = {};
	      	if(dataStart > 0){
				var d = cheerio.load(data.substring(dataStart, data.indexOf(REGISTRATION_HTML_END, dataStart) + REGISTRATION_HTML_END.length))

				var titleTag = d('title');

				dataObject.success = true;
				//dataObject.success = !!titleTag.text().match(/confirmation/i);
				dataObject.error = !!titleTag.text().match(/signup[\s]*error/i);
				//dataObject.response = data;
	  		}
	  		callback(dataObject);
	  		
	    })
	  });
  	httpreq.write(postData);
  	httpreq.end();
}