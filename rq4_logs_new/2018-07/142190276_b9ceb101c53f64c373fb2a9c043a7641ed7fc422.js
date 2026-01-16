var fs = require("fs");
var replaceExt = require('replace-ext');

var filesPath = "public/tvpwidget";
fs.readdir(filesPath, function(err, items) {
    if (items && items.length) {
        for (var i=0; i<items.length; i++) {
            var filePath = filesPath + "/" + items[i] + "/index.html";
            
            if (fs.existsSync(filePath)) {
                var newFilePath = replaceExt(filePath, '.js');
                console.log("replacing ", filePath);
                fs.rename(filePath, newFilePath, function(err) {
                    if (err) console.log('Something went bad ' + err);
                });
            } else {
                console.warn("no html file found ", filePath);
            }
        }
    }
});