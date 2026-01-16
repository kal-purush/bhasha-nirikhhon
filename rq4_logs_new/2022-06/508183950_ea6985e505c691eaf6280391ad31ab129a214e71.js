const express = require('express');
const app = express();
const Port = 8000;

const cors = require('cors');
const mysql = require('mysql');

// const con = mysql.createConnection({
//     host: 'localhost',
//     user: 'root',
//     password: '1234'
// });

// con.connect(function(err) {
//     if(err) throw err;
//     console.log('db 연결 성공!');
// });

app.use(cors())
app.use(express.json());
app.use(express.urlencoded({extended:true}))

const options = {
    host: 'localhost',
    user: 'root',
    password: '1234',
    database: 'blog'
}

const db = mysql.createPool(options)

// db.connect(function(err){
//     if(err) console.log('db연결 실패');
//     console.log('db 연결 성공')
// })

//글등록
app.post('/api/createpost',(req,res) => {

    const title = req.body.title;
    const content=req.body.content;
    const sql = 'insert into post (title,content) values(?,?)'
    db.query(sql,[title,content],(err)=>{
        if(err) {
            console.log('insert err')}
        res.end()
    })
    // console.log(title,content)
    res.end()
})

//글목록 보여주기
app.get('/api/readpostlist',(req,res)=>{
    const sql = 'select * from post'
    db.query(sql,(err,result)=>{
        console.log(result)
        res.send(result)
    })
})

//글 상세정보 보여주기
app.get('/api/readpost/:id',(req,res)=>{
    console.log(req.params.id)
    const id = req.params.id
    const sql = `select * from post where id = ?`
    db.query(sql,[id],(err,result)=>{
        if(err) console.log('detial post err')
        res.json(result)
    })
})

//글 수정
app.put('/api/updatepost/:id',(req,res)=>{
    const id = req.params.id
    const title = req.body.title;
    const content=req.body.content;
    const sql = `update post set title = ? , content = ? where id = ?`
    db.query(sql,[title,content,id],(err,result)=>{
        if(err) console.log("update err")
        res.end()
    })
    
})

//글 삭제
app.delete(`/api/deletepost/:id`,(req,res)=>{
    const id = req.params.id
    const sql = `delete from post where id = ?`
    db.query(sql,[id],(err)=>{
        if(err) console.log('delete err')
        res.end()
    })
})

app.listen(Port, () => {
    console.log(Port,"connected")
})