(function(console){

console.save = function(data, filename){

    if(!data) {
        console.error('Console.save: No data')
        return;
    }

    if(!filename) filename = 'console.json'

    if(typeof data === "object"){
        data = JSON.stringify(data, undefined, 4)
    }

    var blob = new Blob([data], {type: 'text/json'}),
        e    = document.createEvent('MouseEvents'),
        a    = document.createElement('a')

    a.download = filename
    a.href = window.URL.createObjectURL(blob)
    a.dataset.downloadurl =  ['text/json', a.download, a.href].join(':')
    e.initMouseEvent('click', true, false, window, 0, 0, 0, 0, 0, false, false, false, false, 0, null)
    a.dispatchEvent(e)
 }
})(console)



$(function () {
    console.log('Page loaded');
    
    var restaurants_list = [];
    var restaurants_info = [];
    var restaurants_merge = {};
    var restaurants = [];
    
    $.getJSON('../../resources/dataset/restaurants_list.json', function (json) {
        restaurants_list = json;
        Papa.parse('../../resources/dataset/restaurants_info.csv', {
            download: true,
            skipEmptyLines: true,
            header: true,
            complete: function (results) {
                restaurants_info = results['data']
                
                // construct extended json object
                restaurants_list.forEach(function (element) {
                    var id = element['objectID'].toString();
                    restaurants_merge[id] = element;
                });
                
                restaurants_info.forEach(function (element) {
                    var id = element['objectID'].toString();
                    var merge = $.extend({}, restaurants_merge[id], element);
                    restaurants_merge[id] = merge;
                });
                
                Object.getOwnPropertyNames(restaurants_merge).forEach(function (key) {
                    restaurants.push(restaurants_merge[key]);
                });
                
                // save to document to upload to index
                console.save(JSON.stringify(restaurants));
            }
        });
    });
});