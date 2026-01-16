(function (factory) {
    if (typeof module === "object" && typeof module.exports === "object") {
        var v = factory(require, exports);
        if (v !== undefined) module.exports = v;
    }
    else if (typeof define === "function" && define.amd) {
        define(["require", "exports", "chance", "../../secure_hash"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var Chance = require("chance");
    var secure_hash_1 = require("../../secure_hash");
    var SimpleStream = (function () {
        function SimpleStream(host, port) {
            if (host === void 0) { host = "localhost"; }
            if (port === void 0) { port = "8080"; }
            this._magicToken = new Chance().guid();
            this._mySecureHash = secure_hash_1.generateSecureHash(this._magicToken, new Chance().guid());
            this._host = host;
            this._port = port;
        }
        SimpleStream.prototype.setCommunication = function (communication) {
            this._com = communication;
            this._com.onMessage(this._mySecureHash, this._messageCbk);
        };
        SimpleStream.prototype._messageCbk = function () {
        };
        SimpleStream.prototype.getBCInitDataForCaller = function () {
            return {
                constructorName: "SimpleStream",
                constructorArgs: [this._host, this._port],
                initArgs: []
            };
        };
        return SimpleStream;
    }());
});
//# sourceMappingURL=simple_stream.js.map