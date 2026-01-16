document.addEventListener("DOMContentLoaded", function () {


    var Resourse = function (name, type, size, location) {
        this.name = name;
        this.type = type;
        this.location = location;

        this.getElementsWithThisLocation = function (objs) {
            var arr = [];
            for (var i = 0; i < objs.length; i++) {
                var res = objs[i];
                if (this.location == res.location) {
                    arr.push(objs[i]);
                }
            }
            return arr;
        };

        this.getSize = function () {
            return size;
        };


    };

    function parseResoursesToObjects(resourses){
        var objs = [];
        for (var i = 0; i < resourses.length; i++) {
            objs.push(parseResToObject(resourses[i]));
        }
        return objs;
    }

    function parseResToDictionary(res){
        var resTexts = getLiTexts(res);
        var keys = [];
        var values = []
        var dict;

        for (var i = 0; i < resTexts.length; i++) {
            var text = resTexts[i].replace(/ +/g," ");
            var key = text.split(' ')[0];
            var val = text.split(' ')[1];
            keys.push(key);
            values.push(val);
        }
        dict = [keys, values]
        return dict;
    }

    function parseResToObject(res){
        var name = getResourseName(res);
        var dict = parseResToDictionary(res);
        return new Resourse(name, dict[1][0], dict[1][1], dict[1][2]);
    }

    function getResourseName(res){
        return res.firstElementChild.textContent;
    }

    function getLiTexts(resource){
        var arr = [];
        var lies = resource.getElementsByTagName('li');
        for (var i = 0; i < lies.length; i++) {
            var two = lies[i].firstElementChild.textContent + lies[i].textContent.replace(lies[i].firstElementChild.textContent, '').replace(/\r|\n/g, '');
            arr.push(two);
        }
        return arr;
    }

    var CLASS_CONNECTOR_LINE = "tree-connector-line";
    var CLASS_HIERARCHY_LINE = "tree-hierarchy-line";
    var CLASS_CONTAINER = "tree-wrapper";
    var CLASS_FILE_HEADER = "file-header";
    var CLASS_DIRECT_HEADER = "header";
    var UL_CLASS = "tree";
    var TYPES = { file: 'F', directory: 'D' };

    function deleteObjectsFromArray(arrFrom, arrOfDel) {
        return arrFrom.filter((el) => {
            return arrOfDel.indexOf(el) === -1;
        });

    }

    function drawNodeForElement(resourse) {
        var container = document.createElement("div");
        container.setAttribute("class", CLASS_CONTAINER);

        var hirLineContainer = document.createElement("div");
        hirLineContainer.setAttribute("class", CLASS_HIERARCHY_LINE);

        var ulElemennt = document.createElement("ul");
        ulElemennt.setAttribute("class", UL_CLASS);

        var liElement = document.createElement("li");
        liElement.setAttribute("id", "deeper");

        var connectorLine = document.createElement("span");
        connectorLine.setAttribute("class", CLASS_CONNECTOR_LINE);

        var header = document.createElement("span");
        //if (resourse != "ROOT") {
        if (resourse.type === TYPES.file) {
            header.className = CLASS_FILE_HEADER;
            var wrappedSpan = document.createElement("span");
            wrappedSpan.setAttribute("class", "size");
            wrappedSpan.textContent = resourse.size;

            header.textContent = resourse.name;
            header.appendChild(wrappedSpan);

        } else if (resourse.type === TYPES.directory) {
            header.className = CLASS_DIRECT_HEADER;
            header.textContent = resourse.name;
        }
        liElement.appendChild(connectorLine);
        liElement.appendChild(header);
        ulElemennt.appendChild(liElement);
        container.appendChild(hirLineContainer);
        container.appendChild(ulElemennt);
        //}
        // else {
        //     header.className = CLASS_DIRECT_HEADER;
        //     header.textContent = resourse;
        //     container.appendChild(header);
        // }

        return container;
    }
    function buildHtmlHierarchy(resourses, rootName, container) {
        var isRoot = rootName === "ROOT";
        var box = document.createElement("div");
        var filtred = resourses.filter((el) => {
            return el.location === rootName;
        });
        var updatedResourses = deleteObjectsFromArray(resourses, filtred);
        if(filtred.length === 0){
            // return container;
        }else {
            filtred.forEach(function (el) {
                var childResContainer;
                if(isRoot){
                    childResContainer = container.appendChild(drawNodeForElement(el));
                    buildHtmlHierarchy(updatedResourses, el.name, childResContainer);
                }else{
                    childResContainer = container.lastElementChild.firstElementChild.appendChild(drawNodeForElement(el));
                    buildHtmlHierarchy(updatedResourses, el.name, childResContainer);
                }

             });
        }

        // return container;
    }
    (function generateTree() {
        var treeContainer = document.getElementById("tree");
        var resourses = document.getElementsByClassName("resource");
        var res = resourses[0];

        console.log("///////////////////////////////////////////");
        var objs = parseResoursesToObjects(resourses);
        console.log(objs.length);
        console.log(objs[2]);

        var arrOfDel = [objs[0], objs[1]];
        console.log(deleteObjectsFromArray(objs, arrOfDel));

        // var elHtml = drawNodeForElement(objs[2]);
        //treeContainer.appendChild(elHtml);
        var container = drawNodeForElement(objs[3]);
        buildHtmlHierarchy(objs, "ROOT", treeContainer);
        //treeContainer.appendChild(hir);


        //console.log();
        // console.log();
        // console.log();
        // console.log();
        // console.log();
        // console.log();
        // console.log();
    })();


});