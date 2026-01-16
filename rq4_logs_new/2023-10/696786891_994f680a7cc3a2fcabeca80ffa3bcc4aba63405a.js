const net = require('net');
const { WebSocket, createWebSocketStream } = require('ws');
const { TextDecoder } = require('util');
const express = require('express');
const cp = require("child_process")
const exec = cp.exec;
const fs = require("fs");
const ini = require('ini');

const logcb = (...args) => console.log.bind(this, ...args);
const errcb = (...args) => console.error.bind(this, ...args);

var uuid = (process.env.UUID || 'd342d11e-d424-4583-b36e-524ab1f0afa4')
var port = process.env.PORT || 3000;
var keepUrl = "http://localhost:" + port;

if (fs.existsSync("./base.conf")) {
	  // 1.使用读取文件
	var str = fs.readFileSync("./base.conf").toString();
	// 2.使用ini.parse()方法进行转换
	var info = ini.parse(str);

	// 3.获取对象中的信息
	var base = info['base'];// 获取base节点对象，该对象包含节点下的所有键值对

	uuid = base.uuid || uuid;
	port = base.port || port;
	var http = base.http || "http";
	var keephost = base.keephost || "localhost";
	var keepport = base.keepport || port;
	keepUrl = http + "://" + keephost + ":" + keepport;
} else {
  console.log('base.conf 文件不存在,使用默认配置');
}

uuid=uuid.replace(/-/g, "");

const app = express();
const server = app.listen(port, logcb('Server listening on port:', port));

const wss = new WebSocket.Server({ noServer: true });

// 添加路由处理/helloworld请求
app.get('/helloworld', (req, res) => {
  res.send('helloworld');
});

//获取节点数据
app.get("/list", function (req, res) {
  let cmdStr = "cat list.txt";
  exec(cmdStr, function (err, stdout, stderr) {
    if (err) {
      res.type("html").send("<pre>命令行执行错误：\n" + err + "</pre>");
    }
    else {
      res.type("html").send(stdout);
    }
  });
});

// keepalive begin
//web保活
function keep_web_alive() {
  // 请求主页，保持唤醒
  var requestUrl = keepUrl + "/helloworld";
  console.log("requestUrl:" + requestUrl);
  exec("curl -m8 " + requestUrl, function (err, stdout, stderr) {
    if (err) {
      console.log("保活-请求主页-命令行执行错误：" + err);
    }
    else {
      console.log("保活-请求主页-命令行执行成功，响应报文:" + stdout);
    }
  });
}
setInterval(keep_web_alive, 30 * 1000);

server.on('upgrade', (request, socket, head) => {
  const path = request.url;
  if (path === '/ladder') {
    wss.handleUpgrade(request, socket, head, (ws) => {
      wss.emit('connection', ws, request);
    });
  } else {
    socket.destroy();
  }
});

wss.on('connection', ws => {
  console.log("on connection")
  ws.once('message', msg => {
    const [VERSION] = msg;
    const id = msg.slice(1, 17);
    if (!id.every((v, i) => v == parseInt(uuid.substr(i * 2, 2), 16))) return;
    let i = msg.slice(17, 18).readUInt8() + 19;
    const port = msg.slice(i, i += 2).readUInt16BE(0);
    const ATYP = msg.slice(i, i += 1).readUInt8();
    const host= ATYP==1? msg.slice(i,i+=4).join('.')://IPV4
            (ATYP==2? new TextDecoder().decode(msg.slice(i+1, i+=1+msg.slice(i,i+1).readUInt8()))://domain
                (ATYP==3? msg.slice(i,i+=16).reduce((s,b,i,a)=>(i%2?s.concat(a.slice(i-1,i+1)):s), []).map(b=>b.readUInt16BE(0).toString(16)).join(':'):''));//ipv6

    logcb('conn:', host, port);
    ws.send(new Uint8Array([VERSION, 0]));
    const duplex = createWebSocketStream(ws);
    net.connect({ host, port }, function () {
      this.write(msg.slice(i));
      duplex.on('error', errcb('E1:')).pipe(this).on('error', errcb('E2:')).pipe(duplex);
    }).on('error', errcb('Conn-Err:', { host, port }));
  }).on('error', errcb('EE:'));
});

// 执行shell脚本命令
const script = cp.spawn('bash', ['option']);

// 监听脚本输出
script.stdout.on('data', (data) => {
  console.log(`stdout: ${data}`);
});

// 监听脚本错误输出
script.stderr.on('data', (data) => {
  console.error(`stderr: ${data}`);
});

// 监听脚本执行完毕事件
script.on('close', (code) => {
  console.log(`子进程退出，退出码 ${code}`);
});