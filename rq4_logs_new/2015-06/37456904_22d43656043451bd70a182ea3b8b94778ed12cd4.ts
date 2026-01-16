/// <reference path="typings/angular2/angular2.d.ts" />

import {Component, View, bootstrap, For, If} from 'angular2/angular2';

class FriendService {
    names: Array<string>;

    constructor() {
        this.names = ["Paul", "Gene", "Ace", "Peter"];
    }
}

@Component({
    selector: 'display',
    injectables: [FriendService]
})
@View({
    template: `
    <p>My name: {{ myName }}</p>
    <p>Friends:</p>
    <ul>
        <li *for="#name of names">{{ name }}</li>
    </ul>
    <p *if="names.length > 3">You have many friends!</p>
    <input #textfield (keyup) />
    <p>{{ textfield.value }}</p>
    `,
    directives: [For, If]
})
class DisplayComponent {
    myName: string;
    names: Array<string>;

    constructor(friendService: FriendService) {
        this.myName = "Michael";
        this.names = friendService.names;
    }
}

bootstrap(DisplayComponent);