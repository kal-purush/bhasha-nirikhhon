'use strict';

var DEBUG = process.env.NODE_ENV === 'development';

export default function debug () {

    if (DEBUG) {    	
        [].unshift.call(arguments, '[DEBUG Pheromon] ');
        console.log.apply(console, arguments);
    }
};