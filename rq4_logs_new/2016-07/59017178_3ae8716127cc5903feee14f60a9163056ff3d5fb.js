
window.googleCallback = function(data) {

};

$.ajax({
  url: 'http://ajax.googleapis.com/ajax/services/search/news',
  method: 'GET',
  dataType: 'jsonp',
  data: {
    key: 'ABQIAAAACKQaiZJrS0bhr9YARgDqUxQBCBLUIYB7IF2WaNrkYqF0tBovNBQFDtM_KNtb3xQxWff2mI5hipc3lg',
    q: $('.search-input').val(),
    callback: googleCallback
  }
});