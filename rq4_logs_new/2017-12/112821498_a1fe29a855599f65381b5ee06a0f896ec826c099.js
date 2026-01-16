;(function() {
    
    // Define constructor
    this.DataTable = function() {

        // Create global element references
        this.theader = null;
        this.tbody = null;
        this.tfooter = null;
        
        // Define option defaults
        var defaults = {
            Id: '',
            CssClass: '',
            source: null,
            AutoGenerateColumns: false
        }

        // Create options by extending defaults with the passed in arugments
        if (arguments[0] && typeof arguments[0] === "object") {
            this.options = extendDefaults(defaults, arguments[0]);
        }

    }

    function buildOut() { 
        var content = document.getElementById(this.options.Id);
        var columns = content.children();
    }

//     <div id="grd">
// <div id="columns">
// <div datafield="Id" headertext="ID"></div>
// <div datafield="Folio" headertext="Folio"></div>
// <div datafield="Piezas" headertext="Piezas"></div>
// </div>
// <div id="content"></div>
// </div>

    var jon = '[{"Id":1, "Folio":"OT-001-17", "Piezas":50}, {"Id":2, "Folio":"OT-002-17", "Piezas":808}]';

    var json = JSON.parse(jon);

    var arrMapCols = [];

    var MapCol = function(idx, name) {
    this.Idx = idx;
    this.Name = name;
    }

    function myFunction() {
            
        var tbl = document.createElement('table');
        var tblH = document.createElement('thead');
        var tblb = document.createElement('tbody');
        var numRow = 0;
        var cellTbl = 0;
        
        var c = document.getElementById('grd').children;
        var txt = "";
        var i;
        
        arrMapCols = [];
        
        document.getElementById('content').innerHTML = '';
        
        for (i = 0; i < c.length; i++) {
            var div = c[i];
            if(div.attributes.id != undefined)
            switch(div.attributes.id.value) {
                case 'columns':
                    var columns = document.getElementById('columns').children;
                    var row = tblH.insertRow(numRow);
                    for(var col = 0; col < columns.length; col ++) {
                        var column = columns[col];
                        var cellh = document.createElement('th');
                        cellh.innerHTML = column.attributes.getNamedItem('headertext').value;
                        row.appendChild(cellh);
                        var oMapCol = new MapCol(col, column.attributes.getNamedItem('datafield').value);
                        arrMapCols.push(oMapCol);
                        cellTbl++;
                    }
                    for(var idx = 0; idx < json.length; idx ++) {
                    	row = tblb.insertRow(numRow);
                        var objJson = json[idx];
                        console.log(objJson);
                        for(cx = 0; cx < arrMapCols.length; cx ++) {
                        	var v_map_col = arrMapCols[cx];
                        	var cellData = row.insertCell(v_map_col.Idx);
                            cellData.innerHTML = objJson[v_map_col.Name];
                        }
                        numRow++;
                    }
                    break;
            }
            //txt = txt + c[i].getAttribute('id') + "<br>";
        }
        tbl.appendChild(tblH);
        tbl.appendChild(tblb);

        document.getElementById('content').appendChild(tbl);
    }

}());