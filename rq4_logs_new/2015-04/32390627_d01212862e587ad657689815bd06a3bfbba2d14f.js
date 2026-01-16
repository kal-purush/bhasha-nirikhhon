var scriptPram = document.getElementById('offerfinder');
hostName = scriptPram.getAttribute('data-apiurl');
function confirmation(msg,url) {
    var answer = confirm(msg)
    if (answer){
        window.location = url;
    }

}

function spiderWebsite()
{
    var answer = confirm('Are you sure? Dont do this to often, because it will fetch ' +shopName.toUpperCase());
    if(answer) {
        $.ajax({
            context: document.body,
            type: 'GET',
            url: hostName+'/api/run_spider/'+shopName,
            success: function (data) {
                $(".loading").after("Job will be run! Please check back in 1 minute for the updated actions");
            }
        });
    }

}
var QueryString = function () {
    // This function is anonymous, is executed immediately and
    // the return value is assigned to QueryString!
    var query_string = {};
    var query = window.location.search.substring(1);
    var vars = query.split("&");
    for (var i=0;i<vars.length;i++) {
        var pair = vars[i].split("=");
        // If first entry with this name
        if (typeof query_string[pair[0]] === "undefined") {
            query_string[pair[0]] = pair[1];
            // If second entry with this name
        } else if (typeof query_string[pair[0]] === "string") {
            var arr = [ query_string[pair[0]], pair[1] ];
            query_string[pair[0]] = arr;
            // If third or later entry with this name
        } else {
            query_string[pair[0]].push(pair[1]);
        }
    }
    return query_string;
} ();
if(typeof QueryString.updated  === 'undefined') {
    updated = 0;
} else {
    updated = QueryString.updated;
}

if(typeof QueryString.website  === 'undefined') {
    shopName = "cupones";
} else {
    shopName = QueryString.website;
}
if(typeof QueryString.deleted  === 'undefined') {
    contextMenuString = "Delete this row";
    deleted = 0;
} else {
    contextMenuString = "Redo this row";
    deleted=1;
}
$.ajax({
    context: document.body,
    type: 'GET',
    url: hostName+'/api/getcontent/'+shopName+'/'+updated+'/'+deleted,

    success: function (data) {
        $("#loading").hide();
        hot.loadData(data);
    }
});
var
    container = document.getElementById('example1'),
    exampleConsole = document.getElementById('example1console'),
    autosave = document.getElementById('autosave'),
    load = document.getElementById('load'),
    save = document.getElementById('save'),
    autosaveNotification,
    emailValidator,
    hot;

function negativeValueRenderer(instance, td, row, col, prop, value, cellProperties) {
    Handsontable.renderers.TextRenderer.apply(this, arguments);
    if(col == 2) {
        td.style.fontWeight = 'normal';
        td.style.color = 'black';
        td.style.background = '#f3c0c0';
    }

    if(col == 1  || col ==0) {
        cellProperties.readOnly = true; // make cell read-only if it is first row or the text reads 'readOnly'
    }
    if (col == 2) {
        // add class "negative"
        ajax(hostName+'/api/check_content/'+shopName, 'POST','content_hash='+encodeURIComponent(value),  function (res) {
            if(res.responseText=="0") {

                td.style.fontWeight = 'normal';
                td.style.color = 'black';
                td.style.background = '#bfecc7';
            }
        });


    }
}
Handsontable.renderers.registerRenderer('negativeValueRenderer', negativeValueRenderer);
hot = new Handsontable(container, {
    startRows: 1,
    startCols: 1,
    rowHeaders: true,
    colHeaders: true,
    minSpareRows: 0,
    contextMenu: {
        callback: function (key, options) {
            ajax(hostName+'/api/delete_code', 'POST', "delete="+deleted+"&oldValue="+this.getDataAtCell(options.end.row,2)+"&shopName="+shopName, function (res) {

            });
            this.alter('remove_row',options.end.row);
        },
        items: {
            "about": {name: contextMenuString}
        }
    },
    colWidths: [200, 200, 800, 120],
    colHeaders: ["Competitor", "Shopname", "Description","Expiredate"],
    columnSorting: {
        column: 1,
        sortOrder:true
    },
    cells: function (row, col, prop) {
        var cellProperties = {};
        cellProperties.renderer = "negativeValueRenderer"; // uses lookup map
        return cellProperties;
    },

    beforeChange:function(change,source) {
        var oldValue = change[0][3];
        var columnIndex = change[0][1];
        if(columnIndex === 'productName') {
            if(oldValue.length<15) {
                alert("The content cannot be smaller then 15 characters");
                change.splice(0,3);
            }
            if(oldValue.length>150) {
                alert("The content cannot be larger then 150 characters");
                change.splice(0,3);
            }
        }

    },

    afterChange: function (change, source) {
        if (source === 'loadData') {
            return; //don't save this change
        }
        var oldValue = encodeURIComponent(change[0][2]);
        var newValue = encodeURIComponent(change[0][3]);
        var rowIndex = change[0][0];
        var columnIndex = change[0][1];
        if(oldValue != newValue) {
            if (columnIndex == "endDate") {
                ajax(hostName + '/api/updatecode', 'POST', "newValue=" + newValue + "&shopName=" + shopName + "&oldValue=" + hot.getDataAtCell(rowIndex, 2) + "&dateChange=1", function (res) {
                });
            } else {
                if (newValue.length > 15 && newValue.length < 150) {
                    ajax(hostName + '/api/updatecode', 'POST', "newValue=" + newValue + "&shopName=" + shopName + "&oldValue=" + oldValue, function (res) {

                    });
                }
            }
        }
    }
});