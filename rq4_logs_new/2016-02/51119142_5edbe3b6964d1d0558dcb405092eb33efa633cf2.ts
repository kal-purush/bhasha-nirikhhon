// this import should be first in order to load some required settings (like globals and reflect-metadata)
import {nativeScriptBootstrap} from 'nativescript-angular/application';
import {HTTP_PROVIDERS} from 'angular2/http';

import {MainPage} from './main-page';

nativeScriptBootstrap(MainPage, [HTTP_PROVIDERS]);