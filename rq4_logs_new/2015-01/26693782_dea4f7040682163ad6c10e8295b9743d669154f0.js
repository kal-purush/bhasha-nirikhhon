(function(){
    var google = "https://script.google.com/macros/s/AKfycbz6oLGBd7N1rNmxcOzClQ0SWHyAd8z37bXTeAy9UkmMmj6MzD4/exec";
    var displayUrl = google + "?time=?";

    function twoDigits(n) {
        return n > 9 ? "" + n: "0" + n;
    }

    function getDateObject(timeString) {
        var t = timeString.split(/-|T|:|\.|Z/g);
        var d = new Date(t[0],t[1],t[2],t[3],t[4],t[5],t[6]);
        return d;
    }

    function getDuration(timeString1, timeString2) {
        var t1 = getDateObject(timeString1);
        var t2 = getDateObject(timeString2);
        var t1ms = t1.getTime();
        var t2ms = t2.getTime();

        var durationInMs = (t2ms > t1ms) ? t2ms - t1ms: t1ms - t2ms;
        var duration = Math.round(durationInMs / 1000);
        var h = Math.floor(duration / 3600);
        var m = Math.floor((duration - h * 3600) / 60);
        var s = duration - h * 3600 - m * 60;
        return twoDigits(h) + ":" + twoDigits(m) + ":" + twoDigits(s);
    }

    function getTimeString(time) {
        var timezone = 1;
        var hour = Number(time[11])*10 + Number(time[12]) + timezone;
        if (hour == 24) { hour = 0; }
        var min = Number(time[14])*10 + Number(time[15]);
        var output = twoDigits(hour) + ':' + twoDigits(min);
        return output;
    }

    function outputRecords(dataArray) {
        var i, output = [];
        for (i = 0; i < dataArray.length; i++) {
            output.push('<tr>');
            output.push('<td>' + dataArray[i].Name + '</td>');
            output.push('<td>' + getTimeString(dataArray[i].Begin) + '</td>');
            output.push('<td>' + getTimeString(dataArray[i].End) + '</td>');
            output.push('<td>' + getDuration(dataArray[i].Begin, dataArray[i].End) + '</td>');
            output.push('</tr>');
        }
        return output.join("");
    }

    $.getJSON(displayUrl).done(function(data) {
        var content = outputRecords(data);
        var idName = 'time';
        document.getElementById(idName).innerHTML = content;
    });

})();