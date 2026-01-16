var homeTpl = require('./example.hbs');

var html = homeTpl({ data:'整个页面的中间，可以放一些内容之类的东西 '});
$('#bhs').html(html)