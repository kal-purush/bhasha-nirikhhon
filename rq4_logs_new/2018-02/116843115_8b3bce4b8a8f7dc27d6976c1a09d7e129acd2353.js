var express=require('express');
var app =express();

var json={
		code:'200',
		data:{
			a:123
		},
		msg:'请求成功'
}

app.get('/ajax',function(req,res){
	console.log(req)
	res.status(200),
	res.json(json)
});
// app.get('/ajax',function(req,res){
// 	res.status(200),
// 	res.json(json)
// });
// app.get('/ajax',function(req,res){
// 	res.status(200),
// 	res.json(json)
// });
var server = app.listen(3000)