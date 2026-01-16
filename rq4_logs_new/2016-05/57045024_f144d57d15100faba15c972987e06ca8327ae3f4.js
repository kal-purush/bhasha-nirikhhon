function pageTo(data,htmlId){
	var page = plus.webview.getWebviewById(htmlId);
	mui.fire(page,'params',{
		id:data
	});
	//打开详情页面
	mui.openWindow({id: htmlId});
}