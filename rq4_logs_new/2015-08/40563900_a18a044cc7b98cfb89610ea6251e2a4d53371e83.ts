/// <reference path="~/../../typings/angular2/angular2.d.ts" />
import {Component, View, bootstrap, NgFor} from 'angular2/angular2';


module testApp {
    @Component({
        selector: 'testApp'
    })

    @View({
            templateUrl: "/testApp.html",
            directives: [NgFor]
    })
    export class testAppController {
        public welcomeMessage: string;
        public items: Array<string>;

        constructor() {
            var self = this;
            self.welcomeMessage = "Hi! Welcome to the test page for Angular2!"
            self.items = [
                "item 1",
                "item 2",
                "item 3",
                "item 4"
            ];
        }

        public AddItem() {
            var self = this;
            var itemNumber = Math.round(Math.random() * 10)
            self.items.push("item " + itemNumber);
        }
    }
    bootstrap(testAppController);
}

