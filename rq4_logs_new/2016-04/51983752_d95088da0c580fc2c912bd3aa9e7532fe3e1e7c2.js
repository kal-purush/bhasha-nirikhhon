var express = require('express');
var router = express.Router();

router.get('/', function (req, res) {

    // var url = 'http://himawari8-dl.nict.go.jp/himawari8/img/D531106/1d/550/';

    var currentDate = new Date();

    var fetchDate = new Date(currentDate.setMinutes(currentDate.getMinutes() - 30));

    var year = fetchDate.getFullYear();
    var month = fetchDate.getMonth() + 1;
    var date = fetchDate.getDate();
    var hour = fetchDate.getUTCHours();
    var minute = fetchDate.getMinutes();

    var hourStr = (hour > 10) ? hour : '0' + hour;
    var minStr = Math.floor(minute / 10) + '0';

    console.log(hourStr, minStr);

    var url = 'http://himawari8-dl.nict.go.jp/himawari8/img/D531106/thumbnail/550/'
        + year + '/'
        + (month < 10 ? '0' + month : month) + '/'
        + date + '/' +
        hourStr + minStr +
        '00_0_0.png';

    console.log(url);

    res.render('himawari/index', {url: url});
});

module.exports = router;