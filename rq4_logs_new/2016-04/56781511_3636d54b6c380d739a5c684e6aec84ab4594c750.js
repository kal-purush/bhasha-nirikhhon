var http = require('http');
var l_request = require('request');

http.createServer(function(req,res){
    var url = require('url');
    var url_parts = url.parse(req.url, true);
    var ur_query = url_parts.query.uri;
    if(!ur_query){
        DefaultResponse(res);
    }else {
        var v_uri = propUri(ur_query);
        HttpPost(v_uri,res);
    }
}).listen(3000, '127.0.0.1')

function DefaultResponse(response, status, content) {
    response.writeHead(200, {'Content-Type': 'text/html'});
    var status = status? `<p>${status}</p>`: ``;
    var content = content? `<p>${content}</p>`: ``;
    var responseString = `<html><body>
    <h1>Status</h1>
    ${status}
    <h1>Content</h1>
    ${content}
    </body>
    </html>
    `;

    response.write(responseString);
    response.end();
}
function HttpPost(v_uri,res){
    l_request.get(v_uri,{timeout: 2000},function (error, response, body) {
        if (!error && response.statusCode == 200) {
            DefaultResponse(res, 'success', body);
        }else if (!error && response.statusCode == 404){
            DefaultResponse(res, 'error_404', '');
        }else{
            DefaultResponse(res, '', error);
        }
    })
}

function propUri(ur_query) {
    if (ur_query.search('http://') == -1 && ur_query.search('https://') == -1) {
            ur_query = 'http://'.concat(ur_query);
    }
    return ur_query;
}