var ucCourses = [];

var sort_by = function(field, reverse, primer){//abc sorting function

	   var key = primer ? 
	       function(x) {return primer(x[field])} : 
	       function(x) {return x[field]};

	   reverse = !reverse ? 1 : -1;

	   return function (a, b) {
	       return a = key(a), b = key(b), reverse * ((a > b) - (b > a));
	     } 
	}

$.ajax({
	url: window.location.protocol + "//is.byu.edu/site/courses/catalogdata.json.cfm",
	dataType: "json"
}).always(function( data, textStatus, errorThrown ) {
	highSchoolData = data['courses']['HIGH SCHOOL'];
	for(var hsCategories in highSchoolData){//all high school categories
		for(var i=0;i<highSchoolData[hsCategories].length;i++){//var coursesPreTypes in hsCategories){//the list of courses under high school categories, by number
			var courseBaseData = {};
			for(var courseDataKey in highSchoolData[hsCategories][i]) {
				if(courseDataKey != "types") courseBaseData[courseDataKey] = highSchoolData[hsCategories][i][courseDataKey];
			}
			if(Object.keys(highSchoolData[hsCategories][i]).indexOf("types") > -1){
				for(var typei in highSchoolData[hsCategories][i]['types']){
					if((function(tags){for(var j=0;j<tags.length;j++){if((/uc approved/i).test(tags[j].name)){return true;}};return false;})(highSchoolData[hsCategories][i]['types'][typei]['tags'][0].data)){
						//console.log("this one is approved! "+highSchoolData[hsCategories][i]['types'][typei]['short-title']);
						var objectExtended = $.extend(true,courseBaseData,highSchoolData[hsCategories][i]['types'][typei]);
						ucCourses.push(objectExtended);
						
					}
				}
			}
		}
	}
	console.log(ucCourses);
	ucCourses.sort(sort_by('short-title', false, function(a){return a.toUpperCase()})); //sorting the array abc order, case sensitive
	for (var course in ucCourses) {
		var displayNode = $("<li></li>"),
			displayA = $("<a></a>"),
			anchorHref = ("http://is.byu.edu/site/courses/description.cfm?title=" + ucCourses[course]['short-title']),
			courseItem = document.createElement("span"),
			courseDataPieces = (/^([^\:]+)\:(.+?)(\([\w\s]+\))?$/i).exec(ucCourses[course]['university-title']),
			shortTitle = courseDataPieces[1],
			textTitle = courseDataPieces[2]
		;
		
		displayA.attr("href", anchorHref);
		displayA.append($('<span class="course-name"></span>').append(textTitle));
		displayA.append($('<span class="course-numer"></span>').append(shortTitle));
		displayNode.append(displayA);
		//courseItem.innerHTML = displayNode;
		$("#courseList").append(displayNode);
	}
});












