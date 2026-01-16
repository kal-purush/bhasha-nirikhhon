'use strict';
/**
 * Created by Greg on 12/17/2016.
 */
require('angular')
    .module('mallet')
    .service('mallet.Log', [Log]);

function Log(){

    var levels = {
        None: 0,
        Error: 1,
        Warning: 2,
        Info: 4,
        Debug: 8
    },
        level = 0;
}