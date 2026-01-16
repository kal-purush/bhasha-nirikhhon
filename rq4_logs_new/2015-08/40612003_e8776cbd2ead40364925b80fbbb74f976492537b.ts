/// <reference path="typings/node/node.d.ts" />
import net = require('net');

class TcpServer{
	server:net.Server;
	public Start(port:number, callback){
		this.server = net.createServer(function(conn){
			conn.write("echo server...");
			conn.on('data',function(data){
				callback(conn, data);
			})
			
			//conn.on('connect', function(){
			//	console.log("net1:", 'client connect')
			//})
			conn.on('end', function(){
				console.log("net1:", 'client close')
				conn.end();
			})
		});
		this.server.listen(port);
	}
	public Close(){
		if(this.server){
			this.server.close();
			console.log("net1:","server closed.");
		}
	}
}


export function test(){
	var s = new TcpServer();
	s.Start(81,function(conn,data){
		conn.write(data);
		console.log("net1:","recv", data.toString());
	});
	setTimeout(function(){
		s.Close();
	},10000);
	

	var c = net.connect(81,"127.0.0.1");
	c.write("hi");
	c.end();

}