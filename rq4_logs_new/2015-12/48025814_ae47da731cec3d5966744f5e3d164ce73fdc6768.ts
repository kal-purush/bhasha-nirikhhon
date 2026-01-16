///<reference path="../../../typings/tsd.d.ts"/>

import iHar = require('../../interfaces/iHar');
import iEntry = require('../../interfaces/iEntry');

export = ResponseStatisticsController;

class ResponseStatisticsController {
    public responses = null;

    static $inject = [
        'htManager'
    ];

    constructor(
        public htManager
    ) {
        this.responses = this.getResponses(htManager.har);
    }

    public getResponses(har:iHar) {
        var responses = [];
        _.each(har.log.entries, (entry:iEntry) => {
            responses.push(entry.response);
        });
        console.log(responses);
        return responses;
    }
}