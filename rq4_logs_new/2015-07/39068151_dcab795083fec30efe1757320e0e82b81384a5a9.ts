/**
 * Created by karl on 14/07/15.
 */

/// <reference path='typings/tsd.d.ts' />

'use strict';

import jsend = require('./lib/jsend');
import jsendPartial = require('./lib/jsend-partial');

export function init(conf:{partial:boolean} = {partial: false}):void {
    jsend.init();
    if(conf.partial) {
        jsendPartial.init();
    }
}