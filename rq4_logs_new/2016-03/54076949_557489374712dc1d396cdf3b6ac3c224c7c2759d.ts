import {Component, View} from 'angular2/core';
import {bootstrap} from 'angular2/platform/browser';

@Component({
    selector: 'app'
})
@View({
    templateUrl: 'client/app.html'
})
class RestAPIs {
    getData: string;
    public ip = {};
    public randomNumber = {};
    constructor() {
        this.ip = JSON.parse(this.getIP());
        //this.randomNumber = (this.getRandomNumber());
        //console.log(this.randomNumber);
    }

    getIP() {
        HTTP.call("GET", "http://jsonip.com/",
            function(error, result) {
                if (!error) {
                    Session.set("resIP", result.content);
                }
            });
        return Session.get('resIP');
    }

    getRandomNumber() {
        HTTP.call("GET", "https://qrng.anu.edu.au/API/jsonI.php?length=1&type=uint8",
            function(error, result) {
                if (!error) {
                    Session.set("resRandomNumber", result.data);
                    console.log(result.data);
                }
            });
        return Session.get('resRandomNumber');
    }


}

bootstrap(RestAPIs);