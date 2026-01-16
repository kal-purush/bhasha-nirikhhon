/**
 * Created by sarahhicks on 4/26/16.
 */

import { bootstrap } from "angular2/platform/browser";
import { Component } from "angular2/core";

// Component annotation: at min, defines tag and what to display
@Component({
    selector: 'hello-world',
    template: `
    <ul>
        <li *ngFor="#name of names">Hello {{ name }}</li>
    </ul>
    `
})
    // Component definition class with property/type and value
class HelloWorld {
    names: string[];

    constructor() {
        this.names = ['Sarah', 'Robert', 'John'];
    }
}

// boots the app and first argument is the component
bootstrap(HelloWorld);