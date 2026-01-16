window.onload = function () {
	getID("txt_customername").focus();

	//check version
	console.log("Using Chrome: " + isChrome());


	console.log("Done loading all scripts.");
}
/*global variables*/
var n = "\n";
var na = "N/A";
var rec_num = 0;
var recur_array = [];

function _alert(a) {
	//modified alert function
	alert(a);
}

function getID(id) {
	return document.getElementById(id);
}  

function getCN(class_name) {
	return document.getElementsByClassName(class_name);
}

function _show(elem1, elem2) {
	//args: Button ID, Element 1, Element 2
	getCN(elem1)[0].style.display = 'none';
	getCN(elem2)[0].style.display = 'block';
}  

function isChrome() {
	if (/MSIE 10/i.test(navigator.userAgent)) {
		// This is internet explorer 10
		return false;
	} else if (/MSIE 9/i.test(navigator.userAgent) || /rv:11.0/i.test(navigator.userAgent)) {
		// This is internet explorer 9 or 11
		return false;
	} else if (/Edge\/\d./i.test(navigator.userAgent)){
		// This is Microsoft Edge
		return false;
	} else {
		return true;
	}
}

/*  function _recursion(array) {
		if ( rec_num >= array.length ) {
			return allRadio, true;
		} else {
			if (array[rec_num]) {
				allRadio += array[rec_num];
				console.log(allRadio);
			}
			rec_num += 1;
			_recursion(array);
		}
	}*/

function _checkAllBox() {
	var allRadio = "";

	/*input fields*/
	var customerName = getID('txt_customername').value ? 0 : "• Customer Name" + n;
	var serviceTag = getID('txt_servicetag').value ? 0 : "• Service Tag" + n;
	var systemType = getID('txt_systemtype').value ? 0 : "• System Type" + n;
	var detailedComments = getID('txt_detailedcomments').value ? 0 : "• Detailed Comments" + n;
	var resolution = getID('txt_resolution').value ? 0 : "• Resolution input field" + n;
	var callDuration = getID('txt_callduration').value ? 0 : "• Call Duration" + n;

	/*checkboxes*/
	var restateIssue = ( getID('rdo_restateissueyes').checked || getID('rdo_restateissueno').checked )  ? 0 : "• Restate Issue" + n;
	var accountabilityStatement = ( getID('rdo_accountabilitystatementyes').checked || getID('rdo_accountabilitystatementno').checked )  ? 0 : "• Accountability Statement" + n;
	var warrantyEducation = ( getID('rdo_warrantyeducationyes').checked || getID('rdo_warrantyeducationno').checked )  ? 0 : "• Warranty Education" + n;
	var supportedBoundaries = ( getID('rdo_supportedboundariesyes').checked || getID('rdo_supportedboundariesno').checked )  ? 0 : "• Supported Boundaries" + n;
	var gamePlanCommunicated = ( getID('rdo_gameplancommunicatedyes').checked || getID('rdo_gameplancommunicatedno').checked )  ? 0 : "• Game Plan Communicated" + n;
	var feeBasedCommunicated = ( getID('rdo_feebasedcommunicatedyes').checked || getID('rdo_feebasedcommunicatedno').checked )  ? 0 : "• Fee based Communicated" + n;
	var selfHelpOptionProvided = ( getID('rdo_selfhelpoptionprovidedyes').checked || getID('rdo_selfhelpoptionprovidedno').checked )  ? 0 : "• Self Help Option Provided" + n;
	var phoneNumberProvided = ( getID('rdo_phonenumberprovidedyes').checked || getID('rdo_phonenumberprovidedno').checked )  ? 0 : "• Phone number Provided" + n;
	var deptNameCommunicated = ( getID('rdo_deptnamecommunicatedyes').checked || getID('rdo_deptnamecommunicatedno').checked )  ? 0 : "• Dept Name Communicated" + n;
	var furtherAssistanceNeeded = ( getID('rdo_furtherassistanceneededyes').checked || getID('rdo_furtherassistanceneededno').checked )  ? 0 : "• Further Assistance Needed" + n;
	var callBrandingCommunicated = ( getID('rdo_callbrandingcommunicatedyes').checked || getID('rdo_callbrandingcommunicatedno').checked )  ? 0 : "• Call Branding Communicated" + n;

	//var _array = [customerName, serviceTag, systemType, restateIssue, accountabilityStatement, warrantyEducation, supportedBoundaries, gamePlanCommunicated, feeBasedCommunicated, selfHelpOptionProvided, phoneNumberProvided, deptNameCommunicated, furtherAssistanceNeeded, callBrandingCommunicated, detailedComments, resolution, callDuration];

	if (customerName) { allRadio += customerName }
	if (serviceTag) { allRadio += serviceTag }
	if (systemType) { allRadio += systemType }
	if (restateIssue) { allRadio += restateIssue }
	if (accountabilityStatement) { allRadio += accountabilityStatement }
	if (warrantyEducation) { allRadio += warrantyEducation }
	if (supportedBoundaries) { allRadio += supportedBoundaries }
	if (gamePlanCommunicated) { allRadio += gamePlanCommunicated }
	if (feeBasedCommunicated) { allRadio += feeBasedCommunicated }
	if (selfHelpOptionProvided) { allRadio += selfHelpOptionProvided }
	if (phoneNumberProvided) { allRadio += phoneNumberProvided }
	if (deptNameCommunicated) { allRadio += deptNameCommunicated }
	if (furtherAssistanceNeeded) { allRadio += furtherAssistanceNeeded }
	if (callBrandingCommunicated) { allRadio += callBrandingCommunicated }
	if (detailedComments) { allRadio += detailedComments }
	if (resolution) { allRadio += resolution }
	if (callDuration) { allRadio += callDuration }    

	//console.log(_recursion(_array));

	if ( allRadio ) {
		_alert("The following field(s) are required:" +n+ allRadio);
		return false;
	} else {
		return true;
	}
}

//clear function
function _clear() {
	var x = confirm("Are you sure to clear all inputs?");

	if (x) {  
		var allInputElements = document.getElementsByTagName("input");
		var allTextAreaElements = document.getElementsByTagName("textarea");

		for (var x = 0; x < allInputElements.length; x++) {
			//all input text
			if (allInputElements[x].type == "text") {
				allInputElements[x].value = "";
			}
			//all input radio
			if (allInputElements[x].type == "radio") {
				allInputElements[x].checked = false;
			}
		}

		for (var x = 0; x < allTextAreaElements.length; x++) {
			//all textarea
			allTextAreaElements[x].value = "";
		}

		getID('txt_whynotsupported').disabled = true;

		//focus on first input element
		allInputElements[0].focus();

		return 1;
	} else {
		return 0;
	}
}

//generate preview text
function _generateText() {
	var customerName = getID('txt_customername').value ? "Customer Name: " + getID('txt_customername').value + n : "Customer Name: " + na + n;
	var serviceTag = getID('txt_servicetag').value ? "Service Tag: " + getID('txt_servicetag').value + n : "Service Tag: " + na + n ;
	var serviceRequest = getID('txt_servicerequest').value ? "Service Request: " + getID('txt_servicerequest').value + n : "Service Request: " + na + n ;
	var systemType = getID('txt_systemtype').value ? "System Type: " + getID('txt_systemtype').value + n : "System Type: " + na + n;
	var issue = getID('txt_issue').value ? "Issue: " + getID('txt_issue').value + n: "Issue: " + na + n;
	var restateIssueYes = getID('rdo_restateissueyes').checked ? "Restate: Yes" + n : "";
	var restateIssueNo = getID('rdo_restateissueno').checked ? "Restate: No" + n : "";
	var accountabilityStatementYes = getID('rdo_accountabilitystatementyes').checked ? "Accountability Statement: Yes" + n : "";
	var accountabilityStatementNo = getID('rdo_accountabilitystatementno').checked ? "Accountability Statement: No" + n : "";
	var warrantyEducationYes = getID('rdo_warrantyeducationyes').checked ? "Warranty Education: Yes" + n : "";
	var warrantyEducationNo = getID('rdo_warrantyeducationno').checked ? "Warranty Education: No" + n : "";
	var supportedBoundariesYes = getID('rdo_supportedboundariesyes').checked ? "Supported Boundaries: Yes" + n : "";
	var supportedBoundariesNo = getID('rdo_supportedboundariesno').checked ? "Supported Boundaries: No" + n + "Why Not Supported: " + getID('txt_whynotsupported').value + n : "";
	var gamePlanCommunicatedYes = getID('rdo_gameplancommunicatedyes').checked ? "Game Plan Communicated: Yes" + n : "";
	var gamePlanCommunicatedNo = getID('rdo_gameplancommunicatedno').checked ? "Game Plan Communicated: No" + n : "";
	var feeBasedCommunicatedYes = getID('rdo_feebasedcommunicatedyes').checked ? "Fee Based Communicated: Yes" + n : "";
	var feeBasedCommunicatedNo = getID('rdo_feebasedcommunicatedno').checked ? "Fee Based Communicated: No" + n : "";
	var selfHelpOptionProvidedYes = getID('rdo_selfhelpoptionprovidedyes').checked ? "Self-Help Option Provided: Yes" + n : "";
	var selfHelpOptionProvidedNo = getID('rdo_selfhelpoptionprovidedno').checked ? "Self-Help Option Provided: No" + n : "";
	var phoneNumberProvidedYes = getID('rdo_phonenumberprovidedyes').checked ? "Phone Number Provided: Yes" + n : "";
	var phoneNumberProvidedNo = getID('rdo_phonenumberprovidedno').checked ? "Phone Number Provided: No" + n : "";
	var deptNameCommunicatedYes = getID('rdo_deptnamecommunicatedyes').checked ? "Department Name Communicated: Yes" + n : "";
	var deptNameCommunicatedNo = getID('rdo_deptnamecommunicatedno').checked ? "Department Name Communicated: No" + n : "";
	var furtherAssistanceNeededYes = getID('rdo_furtherassistanceneededyes').checked ? "Further Assistance Needed: Yes" + n : "";
	var furtherAssistanceNeededNo = getID('rdo_furtherassistanceneededno').checked ? "Further Assistance Needed: No" + n : "";
	var callBrandingCommunicatedYes = getID('rdo_callbrandingcommunicatedyes').checked ? "Call Branding Communicated: Yes" + n : "";
	var callBrandingCommunicatedNo = getID('rdo_callbrandingcommunicatedno').checked ? "Call Branding Communicated: No" + n : "";
	var detailedComments = getID('txt_detailedcomments').value ? "Detailed Comments: " + n + getID('txt_detailedcomments').value + n : "Detailed Comments: " + na + n;
	var resolution = getID('txt_resolution').value ? n + "Resolution: " + getID('txt_resolution').value + n : n + "Resolution: " + na + n;
	var callDuration = getID('txt_callduration').value ? "Call Duration: " + getID('txt_callduration').value : "Call Duration: " + na;

	return customerName + serviceTag + serviceRequest + systemType + issue + n + "Call Flow: " + n + restateIssueYes + restateIssueNo + accountabilityStatementYes + accountabilityStatementNo + warrantyEducationYes + warrantyEducationNo + supportedBoundariesYes + supportedBoundariesNo + gamePlanCommunicatedYes + gamePlanCommunicatedNo + feeBasedCommunicatedYes + feeBasedCommunicatedNo + selfHelpOptionProvidedYes + selfHelpOptionProvidedNo + n + "Transfer Procedure: " + n + phoneNumberProvidedYes + phoneNumberProvidedNo + deptNameCommunicatedYes + deptNameCommunicatedNo + n +  "Call Closing:" + n + furtherAssistanceNeededYes + furtherAssistanceNeededNo + callBrandingCommunicatedYes + callBrandingCommunicatedNo + n + detailedComments + resolution + callDuration;
} 


/*button click events*/
getID("btn_close").onclick = function () {
	_show("container_pagetwo", "container_pageone");
}

getID("btn_preview").onclick = function () {
	if ( _checkAllBox() ) {
		_show("container_pageone", "container_pagetwo");
		getID("txt_previewtext").value = "";
		getID("txt_previewtext").value = _generateText();
	}
}


getID('rdo_supportedboundariesyes').onclick = function () {
	getID('txt_whynotsupported').disabled = true;
}  

getID('rdo_supportedboundariesno').onclick = function () {
	getID('txt_whynotsupported').disabled = false;
	getID('txt_whynotsupported').focus();
}

getID('btn_clear').onclick = function () { _clear() }

getID("btn_clearandclose").onclick = function () {
	var x = _clear();

	if (x) {
		getCN("container_pagetwo")[0].style.display = 'none';
		getCN("container_pageone")[0].style.display = 'block';
		getID("txt_customername").focus();
	}
}

getID('btn_copy').onclick = function () {
	if (isChrome()) {
		var range = document.createRange();
		range.selectNode(document.querySelector("#txt_previewtext"));
		window.getSelection().addRange(range);
		try {
			var s = document.execCommand("copy");
			var m = s ? "success" : "no success";
			console.log("copy was " + m);
		} catch (error) {
			console.log("oops.. " + error.message);
		}
		window.getSelection().removeAllRanges(); 
	} else {
		window.clipboardData.setData("Text", getID("txt_previewtext").value);
	}
}

/*
	all ids
	main_div
	mid_div
	lef_col_div
	txt_customername
	txt_servicetag
	txt_servicerequest
	txt_systemtype
	txt_issue
	rdo_restateissueyes
	rdo_restateissueno
	rdo_accountabilitystatementyes
	rdo_accountabilitystatementno
	rdo_warrantyeducationyes
	rdo_warrantyeducationno
	rdo_supportedboundariesyes
	rdo_supportedboundariesno
	txt_whynotsupported
	rdo_gamePlanCommunicatedyes
	rdo_gamePlanCommunicatedno
	rdo_feebasedcommunicatedyes
	rdo_feebasedcommunicatedno
	rdo_selfhelpoptionprovidedyes
	rdo_selfhelpoptionprovidedno
	rit_col_div
	rdo_phonenumberprovidedyes
	rdo_phonenumberprovidedno
	rdo_deptnamecommunicatedyes
	rdo_deptnamecommunicatedno
	rdo_furtherassistanceneededyes
	rdo_furtherassistanceneededno
	rdo_callbrandingcommunicatedyes
	rdo_callbrandingcommunicatedno
	txt_detailedcomments
	txt_resolution
	txt_callduration
	txt_previewtext
	bottom_div
	btn_clear
	btn_preview
	mid_div
	bottom_div
	btn_clearandclose
	btn_close
	btn_copy  
	*/