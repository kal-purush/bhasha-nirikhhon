var filters = [
  ['clear', 'Clear'],
  ['greyscale', 'Greyscale'],
  ['sepia', 'Sepia'],
  // ['gaussianBlur', 'Gaussian Blur'],
  ['herMajesty', 'Her Majesty'],
  ['vintage', 'Vintage'],
  ['sinCity', 'SinCity'],
  ['love', 'Love'],
  ['lomo', 'Lomo'],
  ['clarity', 'Clarity'],
  ['sunrise', 'Sunrise'],
  ['orangePeel', 'Orange Peel'],
  ['jarques', 'Jarques'],
  ['hazyDays', 'Hazy Days'],
  ['nostalgia', 'Nostalgia']];

var fs = require('fs');

var currentFilter = 'clear';

var $filterList = $('[data-filters-list]');
var $wallpaperLoader = $('[data-wallpaper-loader]');

var applyFilterToImage = function (filter) {
  Caman('#wallpaper', function () {
    currentFilter = filter;
    $wallpaperLoader.removeClass('hide');
    this.reset();
    if (filter !== 'clear') {
      this[filter]();
    }
    this.render(function () {
      $wallpaperLoader.addClass('hide');
    });
  });
};

var dataURItoBlob = function (dataURI) {
  var byteString = atob(dataURI.split(',')[1]);
  var ab = new ArrayBuffer(byteString.length);
  var ia = new Uint8Array(ab);
  for (var i = 0; i < byteString.length; i++) {
    ia[i] = byteString.charCodeAt(i);
  }
  return new Blob([ab], {type: 'image/jpg'});
};


window.loadFilterList = function () {
  for (var i = 0; i < filters.length; i++) {
    $filterList.html($filterList.html() + '<option value="' + filters[i][0] + '">' +
      filters[i][1] + '</option>');
  }

  $filterList.on('change', function () {
    // window.alert($(this).val());
    applyFilterToImage($(this).val());
  });
};

window.resetFilterList = function () {
  $filterList.prop('selectedIndex', '0');
};

window.initWallpaper = function () {
  applyFilterToImage('clear');
}

window.saveWallpaper = function (output, callback, isOutputComplete) {
  output = isOutputComplete ? output : output + 'zanen-wallpaper.png';

  var dataURL = $('#wallpaper')[0].toDataURL('image/png').replace(/^data:image\/png;base64,/,'');;
  var buf = new Buffer(dataURL, 'base64'); // decode

  fs.writeFile(output, buf, function(err) {
    if(err) {
      console.log("err", err);
    } else {
      return callback(output);
    }
  });
};