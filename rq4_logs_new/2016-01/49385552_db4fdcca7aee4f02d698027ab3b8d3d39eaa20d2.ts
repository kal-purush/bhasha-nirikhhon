import {AppComponent} from './components/app.component'
import {bootstrap} from 'angular2/platform/browser'
import Servers from './services/servers'

import {HTTP_PROVIDERS} from 'angular2/http' 

bootstrap(AppComponent, [HTTP_PROVIDERS, Servers])