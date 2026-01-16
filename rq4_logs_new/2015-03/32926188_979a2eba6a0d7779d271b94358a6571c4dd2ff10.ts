/// <reference path="../tsd/jquery.d.ts" />
/// <reference path="../tsd/jquery.cookie.d.ts" />
/// <reference path="../tsd/es6-promise.d.ts" />
/// <reference path="../tsd/aurelia.d.ts" />
/// <reference path="untappd.ts" />

import aur = require("aurelia-router");

export class App {
    static inject = [aur.Router];

    constructor(private router: aur.Router) {
        this.router.configure((config) => {
            config.title = "Untappd API";
            config.map([
                { route: ["", "welcome"], moduleId: "welcome", nav: true, title: "Welcome" },
                { route: "dashboard", moduleId: "dashboard", nav: true, title: "Dashboard" },
            ]);
        });
    }
}