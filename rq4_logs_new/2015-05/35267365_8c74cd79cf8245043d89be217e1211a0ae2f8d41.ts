/// <reference path="../typings/node/node.d.ts" />
import util = require('util');

export class RekognitionService {
    private BASE_URL = "http://rekognition.com/func/api";
    private options = {};

    constructor(public apiKey : String, public apiSecret : String) {
        this.options["api_key"] = apiKey;
        this.options["api_secret"] = apiSecret;
        this.options["jobs"] = [
            "face_part", "age", "race", "glass", "mouth_open_wide", "eyes_closed",
            "mustache", "sunglasses", "beauty", "gender", "aggressive",
            "emotion"].join('_');
    }

    infer(photoUrl : String) {
        var params : String = ""
        var requestUrl = util.format("%s/?%s", this.BASE_URL, params)
    }
}