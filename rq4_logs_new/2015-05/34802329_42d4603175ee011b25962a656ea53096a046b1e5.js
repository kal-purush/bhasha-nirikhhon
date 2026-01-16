/* jslint node: true */

var fs = require('fs'),
    nd = require('node-dir'),
    path = require('path'),
    models = require('./models.js'),
    config = require('./config.js'),
    DIR = config.DIR,
    ContainerFilePattern = config.ContainerFilePattern,
    extension = config.extension,
    jsFiles = [],
    containerFiles = [],
    echo = console.log,
    jsPattern = /\.js$/,
    found = 0,
    totalContainerFiles = 0;

echo('Starting our mission');
echo('Searching in', DIR);

function getJsFiles(files) {
    var js = files.filter(function (f) {
        return jsPattern.test(f);
    }),
        jsc = js.map(function (f, i) {
            return new models.File(f, i);
        });
    
    return jsc;
}

function parseContainerFile(content, filename) {
    var patt = /<script/gi,
        extpatt = new RegExp('(<script)((.|[\r\n])+?(</script>))', 'gi'),
        srcpatt = new RegExp('<script .*?src=\"(.+)\".+>+?', 'gi'),
        //srcpatt = new RegExp('src=\"(.+)\".+>+?', 'gi'),
        useFound = 0;
    //let's search
    var isIn = patt.test(content);
    if (isIn) {
        //try to find all matches
        var m = null, uses = [], script, src = null, srcCheck = false;
        do {
            m = extpatt.exec(content);
            if (m) {
                script = m[0];
                src = srcpatt.exec(script);
                if (src) {
                    //check if this src is in our jsFiles.
                    srcCheck = jsFiles.some(checkSrcExistence(path.basename(src[1])));
                    
                    if (!srcCheck) {
                        uses.push(script);
                        useFound++;
                    }
                }
            }
        } while(m);
        if (uses.length) {
            echo('... found ', useFound , ' inline uses in ' + path.basename(filename));
            containerFiles.push(new models.UseFile(new models.File(filename), uses));
            echo('... ' + extension + 's affected ', found++);
        }
    }
    totalContainerFiles++;
}

function checkSrcExistence(compareFilename){
    return function (file) {
        return file.name === compareFilename;
    };
}

function writeFile() {
    //creating formatted string
    var nl = '\r\n';
    function addline(str, strAdd) {
        strAdd = strAdd || '';
        str += strAdd + nl;
        return str;
    }
    var fstr = '', tab = '      ';
    fstr = addline(fstr, 'External JS files used in ' + extension + ' documents');
    fstr = addline(fstr, '------------------------------');
    fstr = addline(fstr, nl);
    fstr = addline(fstr, 'SUMMARY');
    fstr = addline(fstr, '------------------------------');
    fstr = addline(fstr, containerFiles.length + ' ' + extension + ' files affected of ' + totalContainerFiles);
    
    containerFiles.forEach(function (f) {
        fstr = addline(fstr, f.file.name + '  ............  ' + f.uses.length + ' uses');
    });

    fstr = addline(fstr);
    fstr = addline(fstr, '------------------------------');
    fstr = addline(fstr, nl);
   
    
    containerFiles.forEach(function (f) {
        //container file name
        fstr = addline(fstr, extension + ': ' + f.file.name);
        fstr = addline(fstr, f.file.path);
        fstr = addline(fstr);
        fstr = addline(fstr, '-------------------------------------');
        fstr = addline(fstr);
        fstr = addline(fstr, tab + 'inline uses ' + f.uses.length);
        fstr = addline(fstr);
        //uses
        f.uses.forEach(function (u) {
            fstr = addline(fstr, tab + u);
            fstr = addline(fstr, nl);
        });
        fstr = addline(fstr);
        fstr = addline(fstr);
        fstr = addline(fstr, '-------------------------------------');
    });
      
    echo('Writing to file...');
    //write to file
    fs.writeFile('external.txt', fstr, function (err) {
        if (err) throw err;
        echo('File output has been created');
    });
}

function getAllDirectoryFiles(directories, done) {
    var result = [];
    directories.forEach(function (dir, i) {
        nd.files(dir, function (err, files) {
            if (err) throw err;
            var fArr = getJsFiles(files);
            result = result.concat(fArr);
            if (i === 0 && done instanceof Function) {
                done(result);
            }
        });
    });
}

function searchForFileNames(directories, done) {
    directories.forEach(function (dir, i) {
        nd.readFiles(dir, {
            match: ContainerFilePattern
        }, function (err, content, usedInFileName, next) {
            if (err) throw err;
            parseContainerFile(content, usedInFileName);
            next();
        }, function (err) {
            if (err) throw err;
            if (i === 0 && done instanceof Function) {
                echo('Found ' + containerFiles.length + ' ' + extension + ' files with js use');
                done();
            }
        });
    });
}

function start() {
    
    if (!(DIR instanceof Array)) {
        DIR = [DIR];
    }
    
    getAllDirectoryFiles(DIR, function (result) {
        jsFiles = result;
        echo('Found ' + jsFiles.length + ' files');
        searchForFileNames(DIR, writeFile);
    });
}

//execute app
start();