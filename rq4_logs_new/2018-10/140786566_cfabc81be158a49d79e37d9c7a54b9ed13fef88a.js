var express = require('express');
var router = express.Router();
var mysql = require('mysql');

//------------------
// 載入資料庫連結
//------------------
var pool = require('./lib/db.js');


/* GET home page. */
router.get('/', function(req, res, next) {
    //取得使用者傳來的參數
    var serkNo=req.param("serNo");
	
	//刪除資料庫內容
    pool.query('DELETE FROM book where serNo=?', [serNo], function(err, result) {
        if (err){
            res.render('bookDeleteFail', {});     //刪除失敗
        }else{
            res.render('bookDeleteSuccess', {});  //刪除成功
        }
    });
});

module.exports = router;