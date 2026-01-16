import * as angular from 'angular';

import component from './dashboard.component.ts';

import userStats from './../../../components/user/stats/stats.module.ts';
import dealsStats from './../../../components/deals/stats/stats.module.ts';
import designHistory from './../../../components/design/history/history.module.ts';

let module = angular.module('dashboard', [
    userStats.name,
    dealsStats.name,
    designHistory.name
]);

module.component('dashboard', component);

export default module;