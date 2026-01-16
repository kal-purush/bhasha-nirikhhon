/**
 * Created by fish on 2015/4/1.
 */

///<reference path="../typings/node/node.d.ts" />
///<reference path="../typings/lodash/lodash.d.ts" />
///<reference path='../typings/xml2js/xml2js.d.ts' />
///<reference path='../typings/bluebird/bluebird.d.ts' />
///<reference path='../typings/moment/moment.d.ts' />
///<reference path='../typings/underscore.string/underscore.string.d.ts' />

import fs = require("fs");
import http = require("http");
import crypto = require("crypto");

import xml2js = require("xml2js");

var _parser = new xml2js.Parser({normalizeTags:true,explicitArray:false,parseNumbers:false});
var _builder = new xml2js.Builder({
    rootName:"UMPAY",
    renderOpts:{pretty:false},
    xmldec:{version :"1.0",encoding:"GBK"}
});

var parse = Promise.promisify(_parser.parseString,_parser);
var build:any = _.bind(_builder.buildObject,_builder);

class Client{
    private desKey:Buffer;
    private priKey;
    private pubKey;

    constructor(private options){
        _.defaults(options,{
            funCode:"",
            spId:"",
            userId:"",
            host:"",
            port:"",
            verifyRespSign:true,
            desKeyPath:"",
            priKeyPath:"",
            pubKeyPath:""
        });

        var NodeRSA = require('node-rsa');

        if(options.desKeyPath) {
            this.desKey = fs.readFileSync(options.desKeyPath);
        }
        if(options.priKeyPath) {
            this.priKey = new NodeRSA(fs.readFileSync(options.priKeyPath),"pkcs8-private-der",{
                signingScheme: {
                    scheme: 'pkcs1', //scheme
                    hash: 'sha1' //hash using for scheme
                }
            });
        }
        if(options.pubKeyPath) {
            this.pubKey = new NodeRSA(fs.readFileSync(options.pubKeyPath),"pkcs8-public-der",{
                signingScheme: {
                    scheme: 'pkcs1', //scheme
                    hash: 'sha1' //hash using for scheme
                }
            });
        }
    }

    transfer(mobile, amount){
        return new Promise((resolve, reject)=>{
            var obj = this.createSubmitObj(mobile, amount);
            var encrypted = this.encrypt(obj);

            this.send(encrypted).then((rst)=>{
                return this.decrypt(rst).then((resp)=>{
                    if (resp.memo) resp.memo = new Buffer(resp.memo,"base64").toString("gbk");

                    if(resp.retcode === "0000"){
                        resolve(resp);
                    }else{
                        reject(new Error(resp.memo));
                    }
                }).catch((err)=>{
                    reject(err);
                });
            });
        });
    }

    encrypt(obj){
        var str = build(obj);
        obj["SIGN"] = this.priKey.sign(new Buffer(str, "gbk"), "base64");
        var newStr = build(obj);
        var cipher = crypto.createCipheriv("des-ecb", this.desKey, "");
        var rst = cipher.update(newStr);
        rst = Buffer.concat([rst,cipher.final()]);
        return rst.toString("base64");
    }

    decrypt(str){
        var decipher = crypto.createDecipheriv("des-ecb", this.desKey, "");
        var rst = decipher.update(new Buffer(str, "base64"));
        rst = Buffer.concat([rst,decipher.final()]);
        var bodyStr = rst.toString("gbk");
        return parse(bodyStr).then((resp)=>{
            var signStr = resp.umpay.sign;
            var bodyStrWithoutSign = bodyStr.replace(/<SIGN>[\S\s]*<\/SIGN>/,"");
            if(this.options.verifyRespSign) {
                if (!this.pubKey.verify(new Buffer(bodyStrWithoutSign, "gbk"), new Buffer(signStr, "base64"))) {
                    throw new Error("报文体验签失败!");
                }
            }
            return resp.umpay;
        });
    }

    createSubmitObj(mobile, amount){
        var rpId = this.generateRpid();
        return {
            FUNCODE:this.options.funCode,
            SPID:this.options.spId,
            REQDATE:moment().format("YYYYMMDD"),
            REQTIME:moment().format("HHmmss"),
            MOBILENO:mobile,
            USERID:this.options.userId,
            RPID:rpId,
            FEEAMOUNT:amount
        };
    }

    generateRpid(){
        var time = moment().format("YYYYMMDDHHmmssSSS");
        var sid = _s.lpad(_.random(10000000).toString(), 7, "0");
        return this.options.spId + time + sid;
    }

    send(body){
        return new Promise((resolve,reject)=>{
            var options = {
                hostname: this.options.host,
                port: this.options.port,
                path: '/',
                method: 'POST',
                headers: {
                    "Connection":"close",
                    'Content-Type': 'application/x-www-form-urlencoded; charset=GBK',
                    'Content-Length': body.length
                }
            };

            var req = http.request(options, (res)=> {
                var buf;
                res.on('data', (chunk)=> {
                    if(!buf)
                        buf = chunk;
                    else
                        buf = Buffer.concat([buf,chunk]);
                });

                res.on("end",()=>{
                    resolve(buf.toString());
                });
            });

            req.on('error', (e)=> {
                reject(e);
            });

            req.write(body);
            req.end();
        });
    }
}

export = Client;