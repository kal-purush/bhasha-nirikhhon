const { dialog } = require('electron').remote;
const fs = require('fs');

$(function() {
    var html = '';
    $('#loadFile').on('click', function() {
        var files = dialog.showOpenDialog({ properties: ['openFile'] });
        for (var i = 0; i < files.length; i++) {
            var stats = fs.statSync(files[i]);
            html += '<br>';
            html += 'File : ' + files[i];
            html += '<br>';
            html += 'fileSizeInBytes : ' + stats['size'];
            html += '<br>';
            html += 'fileSizeInMegabytes : ' + stats['size'] / 1000000.0 + ' MB';
            html += '<br>';
            html += '<br>';
            html += 'Complete File Stats : ' + JSON.stringify(stats || {}, null, 4);
            html += '<br>';
        }
        $('#stats').html(html);
    });
});