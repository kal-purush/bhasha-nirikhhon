//---------------------------------------------------------
// Support functions

// Add a leading zero to intergers 
function addLeadingZero(val,n) {
    var s = String(val);
    while (s.length < n) {s = "0" + s;}
    return s;
}

//---------------------------------------------------------
// For matrix questions, when clicking on the TD, 
// automatically select the radio button or the 
// multiple-select button.
$(document).ready(function() {
    $(".survey-matrix-td-center").click(function () {
        for (i=0;i<this.children.length;i++) {
            if (this.children[i].type == "checkbox") {
                //this.children[i].checked = !this.children[i].checked;
            } else if (this.children[i].type == "radio") {
                this.children[i].checked = true;
            }
        }
    });
});

//---------------------------------------------------------
// Standardize date and time input
function parseDate(id) {
    var str = $("#" + id).val();
    str = str.split(/[^0-9]/);
    var outstr = "";
    for (i=0;i<3;i++) {
        if (i < 2) {
            outstr+=addLeadingZero(str[i],2);
            outstr+="/";
        } else {
            if (str[i].length < 4) {
                outstr+="20" + addLeadingZero(str[i],2);
            } else {
                outstr+=str[i];
            }
        }
    }
    $("#" + id).val(outstr);
}
function parseTime(id) {
    var str = $("#" + id).val();
    str = str.split(/[^0-9]/);
    var outstr = "";
    for (i=0;i<2;i++) {
        if (i < 1) {
            outstr+=addLeadingZero(str[i],2);
            outstr+=":";
        } else {
            outstr+=addLeadingZero(str[i],2);
        }
    }
    $("#" + id).val(outstr);
}

//---------------------------------------------------------
// Parse integer inputs, i.e. remove all other characters
function parseIntegers(id) {
    var str = parseStrings($("#" + id).val(),/[^0-9]/);
    var outstr = "";
    for (i=0;i<str.length;i++) {
        outstr+=str[i];
    }
    $("#" + id).val(outstr);
}

//---------------------------------------------------------
// For Demographics, parse the participant's name
function parseStrings(str,re) {
    var pstr = str.split(re);
    var outstr = "";
    for (i=0;i<pstr.length;i++) {
        for (j=0;j<pstr[i].length;j++) {
            outstr+=pstr[i][j].toLowerCase();
        }
    }
    return outstr;
}
function parseInitials() {
    var str = parseStrings($("#initials").val(),/[^A-Za-z]/);
    var outstr = "";
    for (i=0;i<str.length;i++) {
        outstr+=str[i].toUpperCase();
        outstr+=".";
    }
    $("#initials").val(outstr);
}
function parseName(id) {
    var str = parseStrings($("#" + id).val(),/[^A-Za-z]/);
    var outstr = "";
    for (i=0;i<str.length;i++) {
        if (i == 0) {
            outstr+=str[i].toUpperCase();
        } else {
            outstr+=str[i].toLowerCase();
        }
    }
    $("#" + id).val(outstr);
}