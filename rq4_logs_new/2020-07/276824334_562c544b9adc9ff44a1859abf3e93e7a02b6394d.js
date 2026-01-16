var http = require('http');

var app = http.createServer((req,res)=>{
    if(req.method === 'GET'){
        setTimeout(()=>{
            res.end("1")
        },1000)
    }
})


app.listen(8900);