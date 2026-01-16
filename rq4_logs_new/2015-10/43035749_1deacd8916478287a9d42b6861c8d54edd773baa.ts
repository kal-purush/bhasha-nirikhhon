/// <reference path="../../../typings/tsd.d.ts" />

/// <reference path="../../api/api.service.ts" />

module Leaderboard {
    var app = angular
        .module("octo.leaderboard", [])
        .directive('octoLeaderboard', () => new LeaderboardDirective())
        .controller("LeaderboardController", LeaderboardController);

    export class LeaderboardDirective implements ng.IDirective {
        public templateUrl: string;
        public restrict: string;
        public scope: any;
        public controller: any;
        public controllerAs: string;
        public bindToController: boolean;

        constructor () {
            this.templateUrl = 'components/leaderboard/leaderboard.html';
            this.restrict = 'E';
            this.scope = {
                players: "="
            };
            this.controller = LeaderboardController;
            this.controllerAs = "vm";
            this.bindToController = true;
        }
    }

    export class LeaderboardController {
    }
}