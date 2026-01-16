import MopidyJS = require('node_modules/mopidy/dist/mopidy.js');
import {Injectable} from "angular2/core";
import {NgZone} from "angular2/core";

@Injectable()
export class Mopidy {

    public mopidy = new MopidyJS.default({
        autoConnect: false,
        webSocketUrl: "ws://192.168.185.200:6680/mopidy/ws",
        callingConvention: "by-position-or-by-name"
    });

    private connectPromise = null;
    private zone = null;

    constructor(zone: NgZone) {
        this.zone = zone;
    }

    public connect() {
        var self = this;
        if (!this.connectPromise) {
            this.connectPromise = new Promise(function (resolve, reject) {
                self.mopidy.once("state:online", function () {
                    resolve(self.mopidy);
                });
                self.mopidy.connect();
            });
        }

        return this.connectPromise;
    }


}