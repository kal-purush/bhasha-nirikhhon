import 'reflect-metadata'
import 'zone.js'

import { enableProdMode } from '@angular/core'
import { platformBrowser } from '@angular/platform-browser'
import { AppModuleNgFactory } from '../../aot/src/app/app.module.ngfactory'
import { AppModule } from './app.module'

declare const __PRODUCTION__: boolean;
if (__PRODUCTION__) {
    enableProdMode();
}

platformBrowser().bootstrapModuleFactory(AppModuleNgFactory)