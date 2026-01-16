import {bootstrap} from 'angular2/platform/browser';
import {awesomeApp} from './app/awesomeApp';
import {HTTP_PROVIDERS} from 'angular2/http';
import 'rxjs/add/operator/map';

bootstrap(awesomeApp, [HTTP_PROVIDERS]);