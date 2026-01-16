$(document).ready(function() {

    var mL = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
    var mS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'June', 'July', 'Aug', 'Sept', 'Oct', 'Nov', 'Dec'];
    var hour = 0;
    var obj = null;
    var sleeptime = 0;

    var currentID = "";
    var hisStatus = function(obj){
        if(obj<3) return "Bad";
        if(obj<8) return "Normal";
        if(obj<12) return "Good";
        return "Bad";
    }
    var addComponent = function(date, hour, status) {
        $('#containers').append(
            "<div class=\"row\"><div class=\"col-xs-6 col-md-4 col-lg-4\"><div class=\"panel panel-blue panel-widget \"><div class=\"row no-padding\"><div class=\"col-sm-3 col-lg-5 widget-left\"><svg class=\"glyph stroked calendar\"><use xlink:href=\"#stroked-calendar\"></use></svg></div><div class=\"col-sm-9 col-lg-7 widget-right\"><div class=\"large\">" +
            date +
            "</div><div class=\"text-muted\">Date</div></div></div></div></div><div class=\"col-xs-6 col-md-4 col-lg-4\"><div class=\"panel panel-orange panel-widget\"><div class=\"row no-padding\"><div class=\"col-sm-3 col-lg-5 widget-left\"><svg class=\"glyph stroked clock\"><use xlink:href=\"#stroked-clock\" /></svg></div><div class=\"col-sm-9 col-lg-7 widget-right\"><div class=\"large\">" +
            hour +
            "</div><div class=\"text-muted\">Sleep Hour</div></div></div></div></div><div class=\"col-xs-6 col-md-4 col-lg-4\"><div class=\"panel panel-teal panel-widget\"><div class=\"row no-padding\"><div class=\"col-sm-3 col-lg-5 widget-left "+status+"\"><svg class=\"glyph stroked dashboard-dial\"><use xlink:href=\"#stroked-dashboard-dial\"></use></svg></div><div class=\"col-sm-9 col-lg-7 widget-right\"><div class=\"large\">" +
            status + "</div><div class=\"text-muted\">Status</div></div></div></div></div></div>");
    }

    setInterval(function() {
            $.ajax({url: 'http://10.32.176.4/Exponential'}).done(function(data) {
                // TODO: add information to web
                obj = JSON.parse(data)[0];
                if (obj.id != currentID) {
                    if(hour < 24){
                        console.log(obj.button);
                        if(obj.button) sleeptime++;
                        hour++;
                    }
                    else{
                        addComponent("04/08/59", sleeptime, hisStatus(sleeptime));
                        hour = 0;
                        sleeptime = 0;
                    }
                }
                currentID = obj.id;
            });

        var date = new Date();
        $('#time-label').text(date.getDate() + " " + mS[date.getMonth()] + " " + date.getFullYear() + " " + date.getHours() + ":" + date.getMinutes() + ":" + date.getSeconds());

        },1000);


});