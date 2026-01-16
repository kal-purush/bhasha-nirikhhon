// 2023/03/07
getCSV(new URL("database.csv", "http://127.0.0.1:5500/NishiureArchives/dist/"));// 本番：パスを変更

//processes about CSV
function getCSV(URLObj: URL) {
    const req = new XMLHttpRequest();
    const url = new URL(URLObj);
    req.open("get", url, true);
    req.send(null);

    //if csv could get, call convertCSVtoArray()	
    req.onload = function () {
        convertCSVtoArray(req.responseText, URLObj);
    }
}
//chage CSV to two-dimensional array
function convertCSVtoArray(str: String, URLObj: URL) {
    let result = [];
    let result2 = [];
    let tmp = str.split("\n");
    if (URLObj.toString() == "http://127.0.0.1:5500/NishiureArchives/dist/database.csv") {//本番：パスの変更
        // create two-dimensional array separate each lows in ","
        for (let i = 0; i < tmp.length; ++i) {
            result[i] = tmp[i].split(',');
        }
        for (let i = 0; i < result.length; i++) {
            for (let o = 0; o < result[i].length; o++) {
                if (result[i][o].indexOf("\r") != -1 && result[i][o] != undefined) {
                    result[i][o] = result[i][o].substr(0, result[i][o].indexOf("\r"))
                }
            }
        }
        makeRelationalList(result);
        makeCatalogList(result);
    }
}

//get parameter from URL
function getParam(name: String, url: string) {
    if (url == "") url = window.location.href;
    name = name.replace(/[\[\]]/g, "\\$&");
    var regex = new RegExp("[?&]" + name + "(=([^&#]*)|&|#|$)"),
        results = regex.exec(url);
    if (!results) return null;
    if (!results[2]) return '';
    return decodeURIComponent(results[2].replace(/\+/g, " "));
}

const manifest = getParam("manifest1", "");
function makeRelationalList(result: String[][]) {
    let manifests = [];
    let manifestCategory = 0;
    if (manifest !== null) {
        manifestCategory = Number(manifest.substr((manifest.indexOf(".json") - 1), 1));
        for (let i = 3; i < result[manifestCategory - 1].length; i++) {
            manifests.push({ author: result[manifestCategory - 1][i], title: result[manifestCategory - 1][++i], url: result[manifestCategory - 1][++i] });
        }
    }
    let otherReferences = document.getElementById("other-references");
    for (let i = 0; i < manifests.length; i++) {
        if (manifests[i].url != manifest && otherReferences !== null) {
            otherReferences.innerHTML += `
                <div class='book-data'>
                    <a class='relation-link' href='./view.html?manifest1=${manifests[i].url}'>
                    <img class='book' src='./img/${manifests[i].author}_thumbnail.jpg' alt='thumbnail image'><br>
                    <p class='des'>${manifests[i].author}<br>${manifests[i].title}</p>
                </div>`;
        }
    }
}

//make catalog list and give it to function making Mirador instance 
function makeCatalogList(result: String[][]) {
    let catalogContents = [];
    let manifestCategory = 0
    if (manifest !== null) {
        manifestCategory = Number(manifest.substr((manifest.indexOf(".json") - 1), 1));
        for (let i = 0; i < result[manifestCategory - 1].length; i++) {
            if (result[manifestCategory - 1][i].indexOf('manifest') != -1) {
                const manifestURL = result[manifestCategory - 1][i];
                catalogContents.push({ manifestId: manifestURL });
            }
        }
    }
    makeMiradorInstance(catalogContents);
}

function makeMiradorInstance(catalogContents: any) {
    //get manifest from query
    let manifests = []
    let miradorProperty: any = {};
    for (let i = 1; i <= 4; i++) {
        if (getParam("manifest" + i, "") != null) {
            manifests.push(getParam("manifest" + i, ""));
        }
    }

    if (manifests.length == 1) { // one literature
        miradorProperty = {
            id: "viewer",
            windows: [
                {
                    loadedManifest: manifests[0]
                }
            ],
            catalog: [{

            }
            ],
            window: {
                highlightAllAnnotations: true,
                sideBarOpen: true,
                panels: {
                    "info": true,
                    "attribution": true,
                    "canvas": true,
                    "annotations": true,
                    "search": true,
                    "layers": false,
                }
            }
        };

        // //add relational literature at catalog of Mirador
        // for (manifest of catalogContents) {
        //     miradorProperty.catalog.push(manifest);
        // }

        //わからん
        for (const manifest of catalogContents) {
            miradorProperty.push();

        }

    } else if (manifests.length == 2) { //two literature
        miradorProperty = {
            id: "viewer",
            windows: [
                {
                    loadedManifest: manifests[0]
                },
                {
                    loadedManifest: manifests[1]
                }
            ],
            catalog: [

            ],
            window: {
                highlightAllAnnotations: true,
                sideBarOpen: false,
                panels: {
                    "info": true,
                    "attribution": true,
                    "canvas": true,
                    "annotations": true,
                    "search": true,
                    "layers": false,
                }
            }
        };

        //add relational literature at catalog of Mirador
        for (manifest of catalogContents) {
            miradorProperty.catalog.push(manifest);
        }

    }
    else if (manifests.length == 3) { //three literature
        miradorProperty = {
            id: "viewer",
            windows: [
                {
                    loadedManifest: manifests[0]
                },
                {
                    loadedManifest: manifests[1]
                },
                {
                    loadedManifest: manifests[2]
                }
            ],
            catalog: [

            ],
            window: {
                highlightAllAnnotations: true,
                sideBarOpen: false,
                panels: {
                    "info": true,
                    "attribution": true,
                    "canvas": true,
                    "annotations": true,
                    "search": true,
                    "layers": false,
                }
            }
        };

        //add relational literature at catalog of Mirador
        for (manifest of catalogContents) {
            miradorProperty.catalog.push(manifest);
        }
    }
    else if (manifests.length == 4) { //four literature(Max)
        miradorProperty = {
            id: "viewer",
            windows: [
                {
                    loadedManifest: manifests[0]
                },
                {
                    loadedManifest: manifests[1]
                },
                {
                    loadedManifest: manifests[2]
                },
                {
                    loadedManifest: manifests[3]
                }
            ],
            catalog: [

            ],
            window: {
                highlightAllAnnotations: true,
                sideBarOpen: false,
                panels: {
                    "info": true,
                    "attribution": true,
                    "canvas": true,
                    "annotations": true,
                    "search": true,
                    "layers": false,
                }
            }
        };

        //add relational literature at catalog of Mirador
        for (manifest of catalogContents) {
            miradorProperty.catalog.push(manifest);
        }
    }
    let miradorInstance = Mirador.viewer(miradorProperty);
}