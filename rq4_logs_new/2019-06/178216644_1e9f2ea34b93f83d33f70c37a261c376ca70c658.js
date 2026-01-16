// 功能：用户搜索：python请求后将结果处理成commodity格式，再将结果返回
// 用户将商品添加listen列表：增加 coomodity item, hook item
// 用户查看已有listen列表：获得全部hook信息

// 轮询商品价格：隔断时间遍历hook item中商品信息，并check价格有无变化，若变化，更新商品信息，并发送邮件提醒

// 将商品移除listen列表: 移除hook item
// 查询某商品详细信息(以便查看历史价格走势):返回commodity item
const express = require('express');
const format = require('string-format')
const puppeteer = require('puppeteer');
const cors = require('cors');
const CREDS = require("./creds");
const app_mail = express();
const config = require('./config.dev');
const moment = require('moment');
moment.locale('zh-CN'); // 中文日期
const bodyParser = require('body-parser');
const jsonParser = bodyParser.json();
require('body-parser-xml')(bodyParser);
const xmlParser = bodyParser.xml();
const xml2js = new (require('xml2js').Builder)();
const shortidgenerator = require('js-shortid'); //time+salt
const morgan = require('morgan');
const fs = require('fs');
const logDirectory = __dirname + '/log';
const mongoose = require('mongoose');
const Schema = mongoose.Schema;
const commoditySchema = new Schema(config.mongoCommoditySchema, {collection: 'commodities'});
const commodityModel = mongoose.model('commodities', commoditySchema);
const accessLogStream = require('file-stream-rotator').getStream(config.logAddress);

fs.existsSync(logDirectory) || fs.mkdirSync(logDirectory);
process.stdout.write = accessLogStream.write.bind(accessLogStream); // redirect console.log(stdout) to process.stderr
app_mail.use(morgan('short', {stream: accessLogStream}));
// app_mail.use(cors());
app_mail.use((req, res, next) => {
    res.setHeader('Access-Control-Allow-Origin', '*')
    res.setHeader(
      'Access-Control-Allow-Headers',
      'Origin, X-Requested-With, Content-Type, Accept, Authorization'
    )
    res.setHeader(
      'Access-Control-Allow-Methods',
      'GET, POST, PATCH, DELETE, OPTIONS, PUT'
    )
    next()
  })
Date.prototype.Format = function(fmt) { //author: meizz
    var o = {
        "M+": this.getMonth() + 1, //月份
        "d+": this.getDate(), //日
        "h+": this.getHours(), //小时
        "m+": this.getMinutes(), //分
        "s+": this.getSeconds(), //秒
        "S": this.getMilliseconds() //毫秒
    };
    if (/(y+)/.test(fmt)) fmt = fmt.replace(RegExp.$1, (this.getFullYear() + "").substr(4 - RegExp.$1.length));
    for (var k in o)
        if (new RegExp("(" + k + ")").test(fmt)) fmt = fmt.replace(RegExp.$1, (RegExp.$1.length == 1) ? (o[k]) : (("00" + o[k]).substr(("" + o[k]).length)));
    return fmt;
}

app_mail.post('/mail', jsonParser, (req, res) => {
    console.log("mail: " + req.body.mail)
    console.log("commodityName: " + req.body.commodityName)
    const mg = require('mailgun-js')({apiKey: config.mailgun.mailgun_apikey, domain: config.mailgun.mailgun_domain})
    const content = "Dear user: \n" + req.body.commodityName + "(" + req.body.url +") from " + req.body.source + " is on sale."
    prepareEmailContent(content, req.body.mail).then(
        result => {
            console.log(result)
            mg.messages().send(result, (sendError, body) => {
                if (sendError) {
                    res.send('400');
                } else {
                    res.send('200');
                }
            });
        }
    )
});

async function prepareEmailContent(content, destMail) {
    return {
        from: `AppetizerIO <noreply@appetizer.io>`,
        to: [destMail], // [user's email address]
        subject: "[Argus] Decrease Notification",
        html: content  || 'none',
    };
}

// catch 404
app_mail.use(function (err, req, res, next) {
    next(err);
    res.status(400).end();
});

// entry point
const port = +process.argv[2] || 8087;
app_mail.listen(port, function () {
    console.log('unified-notifier listening on port ' + port);
    mongoose.connect(config.mongoose.connect.url, config.mongoose.options).then(
        () => {
            console.log("数据库连接成功");
        },
        err => {
            console.log("数据库连接失败:", err);
        }
    );
});
