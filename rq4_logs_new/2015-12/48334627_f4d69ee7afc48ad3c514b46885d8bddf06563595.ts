/// <reference path="./typings/tsd.d.ts" />

import Hull3 = require('./Hull3');
import Mission = require('./Mission');
import View = require('./View');


export function init() {
    Mission.getMissionConfig().then(missionConfig => {
        Hull3.updateFromConfig(missionConfig.Hull3);        
        Mission.updateFromConfig(missionConfig);
        View.init();
    });
}

init();