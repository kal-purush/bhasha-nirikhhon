const dbCon = require("./model/sample.js");
const express = require("express");
const app = express();
const router = express.Router();
const bodyParser= require('body-parser');
app.use(bodyParser.urlencoded({extended: true})) ;
let title = "";
let tmp1 = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>샘플</title>
</head>
<body>
    <ul>
        <li><a href="/">메인</a></li>
        <li><a href="/list">목록</a></li>
        <li><a href="/addSample">샘플 추가</a></li>
    </ul>
    <hr>
`;
let tmp2 = `</body>
</html>`;
app.get('/', (req, res) => {
    res.sendFile(__dirname+"/sampleMain.html");
});
app.get('/list', (req, res) => {
    title = `<h2>샘플 항목</h2>`;
    let ul = `<ul>`;
    dbCon.getSampleList()
        .then((rows) => {
            let li = rows.map((row) => {
                return `<li><a href="/get/${row.no}">${row.name}</a></li>`;
            });
            li.map((tag) => {
                ul = ul + tag;
            });
            ul = ul + `</ul>`;
            res.send(tmp1+title+ul+tmp2);
        })
        .catch((errMsg) => {
            res.send(tmp1+title+errMsg+tmp2);
        });
});
app.get('/get/:no', (req, res) => {
    title = `<h2>샘플 상세보기</h2>`;
    let body = "";
    dbCon.getSample(req.params.no)
        .then((row) => {
            let body = row.map((data) => {
                return `<li><a href="/get/${data.no}">${data.name}</a></li>`;
            });
            let btn = row.map((data) => {
                return `<hr><a href="/sampleUpdate/${data.no}/${data.name}">수정</a>
                <hr><a href="/sampleDelete/${data.no}">삭제</a>`;
            });
            res.send(tmp1+title+body+btn+tmp2);
        })
        .catch((errMsg) => {
            res.send(errMsg);
        });
});
app.get('/addSample', (req, res) => {
    res.sendFile(__dirname+"/sampleForm.html");
});
app.post('/addSamplePro', (req, res) => {
    let sample = {no:req.body.no, name:req.body.name};
    console.log(sample);
    dbCon.addSample(sample)
    .then((msg) => {
        console.log(msg);
        res.redirect("/list");
    })
    .catch((errMsg) => {
        console.log(errMsg);
    });
});
//정보 변경 페이지 로딩
app.get('/sampleUpdate/:no/:name', (req, res) => {
    title = `<h2>샘플 수정하기</h2>`;
    let form = `
    <form action="/sampleUpdatePro" method="post">
        <p><input type="hidden" name="no" value="${req.params.no}" placeholder="no hidden"></p>
        <p><input type="text" name="name" value="${req.params.name}" placeholder="name input"></p>
        <p><input type="submit"></p>
    </form>
    `;
    res.send(tmp1+title+form+tmp2);
});
//정보 변경(Update)
app.post("/sampleUpdatePro", (req, res) => {
    let sample = {no:req.body.no, name:req.body.name};
    console.log(sample);
    dbCon.editSample(sample)
    .then((msg) => {
        console.log(msg);
        res.redirect("/list");
    })
    .catch((errMsg) => {
        console.log(errMsg);
    });
});
//정보삭제(delete)
app.get("/sampleDelete/:id", (req, res) => {
    let id = req.params.id;
    dbCon.delSample(id)
    .then((msg) => {
        console.log(msg);
        res.redirect("/list");
    })
    .catch((errMsg) => {
        console.log(errMsg);
    });
});
let port = 4000;
app.listen(port, () => {
    console.log(`Sever Starting on ${port}`);
});