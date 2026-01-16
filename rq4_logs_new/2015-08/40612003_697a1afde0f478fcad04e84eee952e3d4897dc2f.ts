/// <reference path="typings/node/node.d.ts" />
import fs = require('fs');
import crypto = require('crypto');

export function md5(input:string):string{
	var md5 = crypto.createHash("md5");
	md5.update(input);
	return md5.digest("hex");

}

export function md5file(file:string, callback )  {
	fs.readFile(file, function(err, buf) {
		var md5 = crypto.createHash("md5");
		md5.update(buf);
		callback(md5.digest("hex"));
	});
}



export function test(){
	console.log(md5("1"))
	md5file('./md5_1.ts',console.log);
}