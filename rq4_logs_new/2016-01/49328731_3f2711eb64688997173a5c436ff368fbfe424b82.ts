import {bootstrap} from 'angular2/platform/browser';
import {enableProdMode} from 'angular2/core';
import {ROUTER_PROVIDERS} from 'angular2/router';
import {HTTP_PROVIDERS} from 'angular2/http';
import {App} from "./components/app";
import {PropertyService} from "./services/propertyService";
import 'rxjs/Rx';

enableProdMode();
bootstrap(App, [ROUTER_PROVIDERS, HTTP_PROVIDERS, PropertyService]);