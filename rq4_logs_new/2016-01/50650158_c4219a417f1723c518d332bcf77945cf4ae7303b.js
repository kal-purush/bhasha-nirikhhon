var http = require("http");
var url = require('url');
var proxyUrl = null;
    //"web-proxy.usa.hp.com";
var proxyPort = 8080;
//http://ud.cq.qq.com/api/activity/13451/vote/image/451092/jsonp
var opt = {  
        method: "POST",  
        host: "web-proxy.usa.hp.com",
		port:8080,
        path: "http://ud.cq.qq.com/api/activity/13455/vote/image/451098/jsonp",  
        headers: {  
            "Content-Type": 'application/javascript;charset=UTF-8', 
			"Accept-Encoding": 'gzip,deflate',
			"User-Agent": "Mozilla/5.0 (Linux; U; Android 4.0.3; zh-cn; M032 Build/IML74K) UC AppleWebKit/534.31 (KHTML, like Gecko) Mobile Safari/534.31",
			"Proxy-Connection": "Keep-Alive",
			"Content-Length": 0,
			Origin:"http://ud.cq.qq.com/",
			DNT:1,
			Host:"ud.cq.qq.com"
		}  
    };  
var proxy_index = 0;
var proxy_index_2 = 0;
	var proxy_list=
		[
			"web-proxy.china.hp.com",
			"web-proxy.jpn.hp.com",
			"web-proxy.atl.hp.com",
			"web-proxy.sgp.hp.com",
			"proxy.jpn.hp.com",
			"proxy.houston.hp.com",
			"web-proxy.cce.hp.com",
			"web-proxy.usa.hp.com",
			"proxy.houston.hpecorp.net"
		];
function vote()
{
	proxy_index = proxy_index % (proxy_list.length-1);
	opt.host = proxy_list[proxy_index];
    opt.path = "http://ud.cq.qq.com/api/activity/13455/vote/image/451098/jsonp";
	//console.log(opt.host);
	
	//req.write("");
	try
	{
		var req = http.request(opt,function(response)
		{  var body = "";  
				/*
				if (response.statusCode == 200) {  
					var body = "";  
					response.on('data', function (data) { body += data; })  
							.on('end', function () { console.log(body);});  
				}  
				else {
					console.log("Net work error:"+response.statusCode);
				}  
				*/
							response.on('data', function (data) { body += data; })  
							.on('end', function () { console.log(body);});  
		});
		req.end();
	}
	catch(ex)
	{
		console.log("Exception:"+ex);
	}
	proxy_index++;
}
function vote_2()
{
	opt.path="http://ud.cq.qq.com/api/activity/13451/vote/image/451092/jsonp";
	//console.log(opt.host);

	//req.write("");
	try
	{	
		var req = http.request(opt,function(response)
		{  var body = "";  
				/*
				if (response.statusCode == 200) {  
					var body = "";  
					response.on('data', function (data) { body += data; })  
							.on('end', function () { console.log(body);});  
				}  
				else {
					console.log("Net work error:"+response.statusCode);
				}  
				*/
							response.on('data', function (data) { body += data; })  
							.on('end', function () { console.log(body);});  
		});
		req.end();
	}
	catch(ex)
	{
		console.log("Exception:"+ex);
	}
	proxy_index++;
}

setInterval(vote,1000*10);
setInterval(vote_2,1000*10);