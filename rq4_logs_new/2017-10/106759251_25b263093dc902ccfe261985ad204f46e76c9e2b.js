// base function
// 信息提示
function makeAlertShow(element, alertType, alertMsg) {

    var $alertElement = $('<div class="alert ' + alertType + ' alert-dismissible" role="alert">\n' +
        '  <button type="button" class="close" data-dismiss="alert" aria-label="Close"><span aria-hidden="true">&times;</span></button>\n<strong>' +
        '</strong> ' + alertMsg + '\n</div>');
    element.append($alertElement);
}

var ALERT_SUCCESS = "alert-success";
var ALERT_INFO = "alert-info";
var ALERT_WARNING = "alert-warning";
var ALERT_DANGER = "alert-danger";

// button使能去使能
function enableButton(element, text) {
    element.removeAttr("disabled");
    element.html(text);
}

function disableButton(element, text) {
    element.attr("disabled", "true");
    element.html(text);
}