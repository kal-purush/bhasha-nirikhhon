/**
 * Created by janarp in summer '17. Updated by hannesg on fall/winter '17
 */

$(document).ready(function () {
    $('#detail-view').hide();
    $.getJSON('resources/resources.json', function (data) {
        var template = $('#main-row-template').html();
        var tBody = $('#resources');
        var lastName = "";
        var lastVersion = "";
        var latest = data.slice();
            for (var i = 0, len = data.length; i < len; i++) {
            var item = data[i];
                if (item.name == lastName && parseFloat(item.version) > parseFloat(lastVersion)) {
                delete latest[i-1];
                }
            lastName = item.name;
            lastVersion = item.version;
            }
        latest = latest.filter(function (item) { return item != undefined });
            for (var i = 0, len = latest.length; i < len; i++) {
            tBody.append(createRow($(template), latest[i]));
            }
        $('.name_and_version').click(function (event) {
            for (var i = 0, len = latest.length; i < len; i++) {
            item = latest[i];
                if (event.target.id == 'resource_'+item.url) {
                tmp = item;
                }
            }
            var temp = $('#detail-view').html();
            var newRow = $(temp);
            updateFields(tmp, data);
            $('#table-view').hide();
            $('#detail-view').show();
        });
        if (location.hash.length>1) {
        $('#resource_'+location.hash.substring(1)).trigger('click');
        }
    });
    $('.backButton').on('click', function (event) {
    $('#table-view').show();
    $('#detail-view').hide();
    location.hash = '';
    });
    $(window).on('hashchange', function() {
        if (location.hash.length>1) {
        $('#resource_'+location.hash.substring(1)).trigger('click');
        }
    });
});


function createRow(newRow, item) {
newRow.find('.name_and_version').text(item.name).attr("id", 'resource_'+item.url).attr('href', '#'+item.url);
newRow.find('.status').text(item.status);
newRow.find('.type').text(item.type);
return newRow;
}

function getFileName(fullpath) {
return fullpath.substring(fullpath.lastIndexOf('/') + 1);
}


function getFileList(files) {
t = '';
    for (var i = 0, len = files.length; i < len; i++) {
    var item = files[i];
    t += '<li><i class="fa fa-external-link text-primary"></i> &nbsp; <a href="resources/'+item+'">'+getFileName(item)+'</a></li>';
    }
return t;
}


function updateFields(item, data) {
$('#name').text(item.name);
$('#status').text(item.status.toLowerCase());
$('#version').html('');
    for (var i = 0, len = data.length; i < len; i++) {
    tmp = data[i];
        if (item.name == tmp.name) {
        $('#version').html($('#version').html()+getVersionButton(tmp, item));
        }
    }
    $('.versionButton').click(function (event) {
        for (var i = 0, len = data.length; i < len; i++) {
        tmp = data[i];
            if (event.target.id == 'version_'+tmp.url+'_'+tmp.version.replace('.', '_')) {
            current = tmp;
            }
        }
    var item = current;
    updateFields(item, data);
    $('.versionButton').attr("class", "btn btn-outline-primary btn-sm versionButton");
    $('#version_'+item.url+'_'+item.version.replace('.', '_')).attr("class", "btn btn-primary btn-sm versionButton");
    });
$('#start_date').text(item.start_date);
$('#end_date').text(item.end_date);
$('#description').text(item.description);
$('#type').text(item.type);
$('#owner').text(item.owner);
$('#files').html(getFileList(item.files));
}


function getVersionButton(item, current) {
id = 'version_'+item.url+'_'+item.version.replace('.', '_');
cssClass = 'btn btn-'+(item.version == current.version ? '' : 'outline-')+'primary btn-sm versionButton';
return '<button type="button" id="'+id+'" class="'+cssClass+'">'+item.version+'</button>';
}