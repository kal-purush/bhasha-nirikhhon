// connect to Cloudant
var cloudant = require('cloudant');
var url = process.env.CLOUDANT_URL || 'http://localhost:5984';
var dbname = process.env.DBNAME || 'pageviews';
var db = cloudant({url:url , plugin: 'promises'}).db.use(dbname);

setInterval(function() {
  var i = Math.floor(Math.random()*255);
  var i2 = Math.floor(Math.random()*255);
  var i3 = Math.floor(Math.random()*255);
  var i4 = Math.floor(Math.random()*255);
  var doc =   {
    "path": "/blog/post-" + i + ".html",
    "date": new Date().toISOString(),
    "mobile": true,
    "browser": "Chrome",
    "ip": [i,i2,i3,i4].join(".")
  };
  db.insert(doc);
}, 40);