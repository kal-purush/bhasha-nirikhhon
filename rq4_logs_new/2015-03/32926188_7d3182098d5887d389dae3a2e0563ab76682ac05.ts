/// <reference path="../../typings/d3/d3.d.ts" />
/// <reference path="../../typings/d3/topojson.d.ts" />
/// <reference path="../../typings/aurelia/aurelia.d.ts" />

import Behavior = require("aurelia-templating");

export class MapKey {
    static metadata(){
        return Behavior
            .customElement('map-key')
            .withProperty('to');
    }

    speak(){
        alert('Hello ${this.to}!');
    }
}