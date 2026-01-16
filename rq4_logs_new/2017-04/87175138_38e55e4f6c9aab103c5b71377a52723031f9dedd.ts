
import request = require('request');
import {IncomingMessage} from 'http';
import * as vscode from 'vscode';

export class HttpClinet {

    private _header : {[key:string]:string} = {};
    private _userEmail : string = null;    

    constructor(private host: string = ''){
        this._header['Content-Type'] = 'application/json';
        if(!this.host) {
            this.host = <string>vscode.workspace.getConfiguration().get('keydiary.url');
        }

        this._userEmail = <string>vscode.workspace.getConfiguration().get('keydiary.email');
    }

    public send(data:any){
        let options  = {
            url: this.host,
            method: 'POST',
            headers: this._header,
            json: true,
            from: data
        }
        let proxy = vscode.workspace.getConfiguration().get('http.proxy');        
        data['UserID'] = this._userEmail;

        if(proxy){
            options['proxy'] = proxy;
            let strictSSL : boolean = <any>vscode.workspace.getConfiguration().get('http.proxyStrictSSL');
            if (strictSSL === true) {
                options['strictSSL'] = strictSSL;                
            }
        }

        request(options, this.sendCallback);
    }

    sendCallback = (error: any, response: IncomingMessage, body) => {
        if(response.statusCode == 200){
            console.log(`Communication was successful.`);
        }else{
            console.error(`Communication to the host (${this.host}) failed: ${response.statusCode}`);
        }
    }
}