/*
var express = require('express');
var app = express();	// 创建框架的类
var fs = require('fs');

var bodyParser = require('body-parser');	// 加载中间件
var multer = require('multer');
var storage = multer.diskStorage({ 
	destination: (req, file, cb) => {
		cb(null, './image')	// 文件上传的目录
	},
	filename: (req, file, cb) => {
		var fileFormat = (file.originalname).split(".");	// 先将文件名采用split按照.进行分裂
		cb(null, file.fieldname + '-' + Date.now() + '.' +  fileFormat[fileFormat.length - 1]);	// 对文件重命名，加上扩展名
	}
});	// 确定一下上传的配置选项


app.use(express.static('public'));	// 添加静态文件，用于处理静态文件，框架自带的
app.use(bodyParser.urlencoded({ extended: false }));	// 用于处理JSON等数据
app.use(multer({storage: storage}).array('image'));

app.get('/index.html', (req, res) => {
	res.sendFile( __dirname + '/' + 'index.html' );	// 对于首页的请求
});

// 该过程中间经历了对静态文件的（static中间件，urlencoded对url的解析，只允许字符串和数字的结果，使用了上传的multer的中间件，最后到回调函数）
app.post('/file_upload', (req, res) => {	// 允许上传多个文件，其中文件数组保存在req.files
	console.log(req.files[0]);	// 上传文件的信息
	 var des_file = __dirname + "/" + req.files[0].originalname;
   fs.readFile( req.files[0].path, function (err, data) {
        fs.writeFile(des_file, data, function (err) {
         if( err ){
              console.log( err );
         }else{
               response = {
                   message:'File uploaded successfully', 
                   filename:req.files[0].originalname
              };
          }
          console.log( response );
          res.end( JSON.stringify( response ) );
       });
   });
});

var server = app.listen(1937, () => {
	console.log(server.address());
});



*/

var express = require('express');
var app = express();
var fs = require("fs");
 
var bodyParser = require('body-parser');
var multer  = require('multer');
 
app.use(express.static('public'));
app.use(bodyParser.urlencoded({ extended: false }));
app.use(multer({ dest: '/tmp/'}).array('image'));	// 设置临时上传的地址
 
app.get('/index.htm', function (req, res) {
   res.sendFile( __dirname + "/" + "index.htm" );
})
 
app.post('/file_upload', function (req, res) {
 
   console.log(req.files[0]);  // 上传的文件信息
 
   var fileFormat = (req.files[0].originalname).split(".");
   var fileName = req.files[0].fieldname + '-' + Date.now() + "." + fileFormat[fileFormat.length - 1];
   var des_file = __dirname + "/image/" + fileName;	// 将要上传的地址

	console.log(des_file);
	fs.open( './image/' + fileName, 'a', function(err, fd) {
   if (err) {
       return console.error(err);
   }
  fs.readFile( req.files[0].path, (err, data) => {
	        fs.writeFile(des_file, data, (err) => {
	        console.log(data);
	         if( err ){
	              console.log( err );
	         }else{
	               response = {
	                   message:'File uploaded successfully', 
	                   filename:req.files[0].originalname
	              };
	          }
	          console.log( response );
	          res.end( JSON.stringify( response ) );
	      	 });
   		});    
});

  		
});
 
var server = app.listen(8081, function () {
 
  var host = server.address().address
  var port = server.address().port
 
  console.log("应用实例，访问地址为 http://%s:%s", host, port)
 
})

