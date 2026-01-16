import * as dojoDeclare from "dojo/_base/declare";
import * as _WidgetBase from "mxui/widget/_WidgetBase";

export class Carousel extends _WidgetBase {
    // Properties from Mendix modeler
    private message: string;

    postCreate() {
        console.log("We have a widget ", this.message);
    }

    update(object: mendix.lib.MxObject, callback: Function) {
        console.log("We have a constext ", object);

        callback();
    }
}

// Declare widget's prototype the Dojo way
// Thanks to https://github.com/DefinitelyTyped/DefinitelyTyped/blob/master/dojo/README.md
// tslint:disable : only-arrow-functions
dojoDeclare("com.mendix.widget.carousel.Carousel", [_WidgetBase], (function (Source: any) {
    let result: any = {};
    for (let i in Source.prototype) {
        if (i !== "constructor" && Source.prototype.hasOwnProperty(i)) {
            result[i] = Source.prototype[i];
        }
    }
    return result;
} (Carousel)));