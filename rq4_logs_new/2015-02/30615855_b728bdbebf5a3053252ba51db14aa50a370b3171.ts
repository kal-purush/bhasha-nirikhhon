/// <reference path="../def/node/node.d.ts" />
import url = require('url');
import crypto = require('crypto');
import querystring = require('querystring');

export class Crypto {

    public static urldecrypt(url_string: string, secret: string) {
        //Extract data from url
        var parsed_url = url.parse(url_string);
        var query_components : any = Crypto.parseQuery(parsed_url.query);

        //Extract decoded data, calculate and encode signature
        var decoded_json: string = this.base64_decode(querystring.unescape(query_components.data));
        var signature = this.base64_encode(Crypto.signString(decoded_json, secret));
        var encoded_signature = querystring.unescape(query_components.key);

        console.log();
        console.log("json: "+decoded_json);
        console.log("sign: "+signature);
        console.log("sign: "+encoded_signature);

        //If calculated signature is valid with given signature
        if(signature == encoded_signature) {
            return decoded_json;
        } else {
            throw new Error("Invalid private key");
        }
    }

    private static parseQuery(query) {
        var query_map = {};

        if (typeof query != 'string') {
            return query_map;
        }

        query.split('&').map(function(el){
            var split_at = el.indexOf('=')
            query_map[el.slice(0, split_at)] = el.slice(split_at+1)
        });

        return query_map;
    }

    public static urlencrypt(payload: any, secret: string) {
        var json_payload = JSON.stringify(payload);
        var signature = this.base64_encode(this.signString(json_payload, secret));

        var encoded_payload = this.base64_encode(json_payload);

        console.log();
        console.log("json: "+json_payload);
        console.log("sign: "+signature);

        return "data="+encoded_payload+"&key="+signature;
    }


    public static signString(string_to_sign: string, shared_secret: string) {
        var hmac: any = crypto.createHmac('sha512', shared_secret);
        hmac.update(string_to_sign);
        hmac.end();
        return hmac.read();
    }

    private static base64_encode(data: string) {
        return new Buffer(data).toString('base64');
    }

    private static base64_decode(data: string) {
        return new Buffer(data, 'base64').toString();
    }
}