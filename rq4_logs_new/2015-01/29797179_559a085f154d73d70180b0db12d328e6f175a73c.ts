import express = require("express");
import bodyParser = require("body-parser");
import xml = require("./xml");
import fs = require("fs");
import path = require("path");
import musicmetadata = require("musicmetadata");
import mime = require("mime");
import ProgressBar = require("progress");

type Done = () => void;
type FileHandler = (filePath: string, done: Done) => void;

function recurseFiles(p: string, each: FileHandler, done: Done) {
    fs.stat(p, function(err, s) {
        if (!err && s.isDirectory()) {
            fs.readdir(p, function(err, ar) {
                if (err) {
                    console.log(err);
                    done();
                    return;
                }
                var n = 0;
                function step() {
                    if (n === ar.length) {
                        done();
                    } else {
                        var child = ar[n++];
                        if (child[0] !== ".") {
                            recurseFiles(path.join(p, child), each, step);
                        } else {
                            step();
                        }
                    }
                }
                step();
            });
        } else {
            each(p, done);
        }
    });
}

var metabasePath = path.join(__dirname, "metabase");
if (!fs.existsSync(metabasePath)) {
    fs.mkdirSync(metabasePath);
}

var coverartPath = path.join(metabasePath, "coverart");
if (!fs.existsSync(coverartPath)) {
    fs.mkdirSync(coverartPath);
}

var libraryPath = process.argv[2];

if (!libraryPath) {
    throw new Error("No library path specified");
}

interface Metadata extends MusicMetadata.Metadata {
    path: string;
    id: number;
    modified: string;
    coverart: string;
}

var metabaseFile = path.join(metabasePath, "metabase.json");
var metabase: Metadata[] = [];
var indices: {
    [indexName: string]: {
        [indexValue: string]: Metadata[];    
    }
} = {};

function forValuesIn(item: any, property: string, handler: (val: any) => void) {
    var vals = item[property];
    if (!Array.isArray(vals)) {
        handler(vals);
    } else {
        vals.forEach(handler);
    }
}

console.log("Loading metabase...");
fs.readFile(metabaseFile, 'utf8', function(err, metabaseJson) {

    console.log("Parsing metabase...");
    var loadedMetabase: Metadata[] = err ? [] : JSON.parse(metabaseJson);
    var changes = !!err;

    console.log("Cross-referencing with " + loadedMetabase.length + " items...");
    var metabaseByPath: { [pathString: string]: Metadata } = {};
    
    var bar = new ProgressBar(':bar', { total: loadedMetabase.length });
    
    loadedMetabase.forEach(function(item) {
        try {
            bar.tick();
            var s = fs.statSync(path.join(libraryPath, item.path));
            if (s) {
                var modified = s.mtime.toISOString();
                if (modified === item.modified) {
                    metabaseByPath[item.path] = item;
                } else {
                    changes = true;
                }
            } else {
                changes = true;
            }
        } catch (x) {
            // Ignore exceptions, probably file has disappeared
            console.log("Disappeared: " + item.path);
            changes = true;
        }
    });

    console.log("Scanning library for changes...");
    recurseFiles(libraryPath, function(filePath, done) {
        var relPath = filePath.substr(libraryPath.length);
        if (relPath[0] === "/") {
            relPath = relPath.substr(1);
        }
        if (metabaseByPath[relPath]) {
            done();
            return;
        }
        console.log("Detected: " + relPath);
        changes = true;
        var parser = musicmetadata(fs.createReadStream(filePath));
        var metadata: MusicMetadata.Metadata;
        parser.on("metadata", function(result) {
            metadata = result;
        });
        parser.on("done", function(ex) {
            var expandedMetadata: Metadata = <Metadata>metadata;
            
            var finish = function () {
                fs.stat(filePath, function(err, s) {
                    if (!err) {
                        expandedMetadata.modified = s.mtime.toISOString();
                        expandedMetadata.path = relPath;
                        metabaseByPath[relPath] = expandedMetadata;
                    }
                    done();
                });
            };
            if (ex || !metadata) {
                // Fake metadata from some assumptions about the path
                var albumPath = path.dirname(relPath),
                    artistPath = path.dirname(albumPath);            
                expandedMetadata = <Metadata>{
                    album: path.basename(albumPath),
                    artist: [path.basename(artistPath)],
                    title: path.basename(relPath)
                };
                finish();
            } else {             
                var pictureData = metadata.picture && metadata.picture[0];
                if (pictureData) {
                    delete metadata.picture;

                    // Only keep one image per folder, as this is almost always correct
                    var imageName = path.dirname(relPath).replace(/[\\\/]/g, "_");
                    imageName += "." + pictureData.format;
                    var imagePath = path.join(coverartPath, imageName);

                    fs.exists(imagePath, function(exists) {
                        if (exists) {
                            expandedMetadata.coverart = imageName;
                            finish();
                        } else {
                            fs.writeFile(imagePath, pictureData.data, function (err) {
                                if (!err) {
                                    expandedMetadata.coverart = imageName;
                                }
                                finish();
                            });
                        }
                    });
                } else {
                    finish();
                }
            }
        });
    }, function() {
        console.log("Compiling metabase...");
        for (var relPath in metabaseByPath) {
            var item = metabaseByPath[relPath];
            metabase.push(item);            
            Object.keys(item).forEach(function(key) {
                var index = indices[key] || (indices[key] = {});                
                forValuesIn(item, key, val => {
                    var contents = index[val] || (index[val] = []);
                    contents.push(item);
                });                
            });
        }

        if (changes) {
            console.log("Saving updated metabase...");                    
            fs.writeFileSync(metabaseFile, JSON.stringify(metabase, null, 4));
        }
        
        console.log("Assigning IDs");
        metabase.forEach(function(item, id) {
            item.id = id;
        });
        
        console.log("Up to date");
    });
});

var app = express();
app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

function api(name: string, func: (query: any, req: Express.Request) => any) {
    app.get("/" + name, function(req, res) {
        var result = func(req.query, req);
        res.set("Content-Type", "application/json");
        res.send(JSON.stringify(result, null, 2));
    });
}

app.get("/coverart", function(req, res) {
    var p = path.join(coverartPath, req.query.name);  
    res.setHeader("Content-Type", mime.lookup(p));
    fs.createReadStream(p).pipe(res);    
});

app.get("/stream", function(req, res) {
    var item = metabase[req.query.id];
    if (!item) {
        res.status(500).send('Bad id');
    } else {
        var p = path.join(libraryPath, item.path);
        res.setHeader("Content-Type", mime.lookup(p));
        fs.createReadStream(p).pipe(res);
    }
});

interface ResultsMaker {
    (indexName: string, add: (value: string, coverart: string, item?: Metadata) => boolean): void;
}

interface Hit {
    value: string;
    coverart: string;
    
    id?: number;
    album?: string;
    artist?: string;
    title?: string;
}

interface Results {
    [indexName: string]: Hit;
}

interface PagingOptions {
    skip?: string;
    take?: string;
}

function makeIndexResults(indexName: string, args: PagingOptions, handler: ResultsMaker) {
        
    var skip = parseInt(args.skip || "0", 10), take = parseInt(args.take || "100", 10);
    var hits: Hit[] = [];
    
    handler(indexName, function(value, coverart, item) {
        if (skip > 0) {
            skip--;
            return true;
        }
        if (take <= 0) {
            return false;
        }
        take--;

        var result: Hit = { value: value, coverart: coverart };
        
        if (item) {
            result.id = item.id;
            result.album = item.album;
            result.artist = item.artist[0];
            result.title = item.title[0];                
        }

        hits.push(result);
        return take > 0;
    });
    
    return hits.length > 0 ? hits : null;
}

function makeResults(args: PagingOptions, handler: ResultsMaker) {
    return {
        genre: makeIndexResults("genre", args, handler),
        artist: makeIndexResults("artist", args, handler),
        album: makeIndexResults("album", args, handler), 
        title: makeIndexResults("title", args, handler)
    };
}

api("fetch", function(args) {
    if (!args.type || !args.name) {
        throw new Error("Requires type, name parameters");
    }
    
    if (args.type === "id") {
        var item = metabase[args.name];
        if (!item) {
            return {};
        }
        return makeResults(args, function(indexName, add) {
            forValuesIn(item, indexName, function(val) {
                add(val, item.coverart);
            });
        });
    }
    
    var index = indices[args.type];
    if (!index) {
        return {};
    }
    var items = index[args.name];
    if (!items) {
        return {};
    }
    return makeResults(args, function(indexName, add) {
        var values: { [key: string]: boolean } = {};                
        items.forEach(function(item) {
            var itemFolder = path.dirname(item.path);
            forValuesIn(item, indexName, function(val) {
                var valKey = val;
                if (indexName === "title") {
                    valKey += "/" + itemFolder;
                }
                if (!values[valKey]) {
                    values[valKey] = true;
                    add(val, item.coverart, indexName === "title" ? item : null);
                }
            });
        });         
    });
});

api("query", function(args) {
    if (!args.value) {
        throw new Error("Requires value parameter");
    }

    var lowerVal = args.value.toLowerCase();
    
    return makeResults(args, function(indexName, add) {
        var index = indices[indexName];
        
        for (var key in index) {
            if (key.toLowerCase().indexOf(lowerVal) !== -1) {

                var items = index[key];

                if (indexName === "title") {
                    items.forEach(function(item) {
                        add(key, item.coverart, item);
                    });
                } else {
                    var coverart: string;
                    items.some(function(item) {
                        return !!(coverart = item.coverart);
                    });
                    add(key, coverart);
                }
            }
        }
    });
});

function subsonic(name: string, func: (query: any, req: express.Request) => Promise<any>) {
    var wrapper = function(args: any, req: express.Request, res: express.Response) {
        
        func(args, req).then(data => {
            
            var response = xml({ 'subsonic-response': data }).toString();
            console.log('Response: ' + JSON.stringify(response, null, 4));
            res.set('Content-Type', 'application/xml');
            res.send(response);    
            
        }).catch(err => {
            res.status(500).send(err.message);            
        });        
    }
    name = '/rest/' + name + '.view';
    app.get(name, function(req, res) {
        wrapper(req.query, req, res);
    });
    app.post(name, function(req, res) {
        wrapper(req.body, req, res);
    });
}

app.use(express.static(__dirname + "/client"));

var port = 3003;
app.listen(port);
console.log("Server running at http://127.0.0.1:" + port + "/");