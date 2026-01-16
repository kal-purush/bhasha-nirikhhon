/// <reference path="../typings/angular2/angular2.d.ts" />

import {bootstrap, ChangeDetection, jitChangeDetection} from "angular2/angular2";
import {bind} from "angular2/di";
import {App} from "components/app/app";
import {Quote} from "components/quote/quote";

bootstrap(App, [bind(ChangeDetection).toValue(jitChangeDetection)]);