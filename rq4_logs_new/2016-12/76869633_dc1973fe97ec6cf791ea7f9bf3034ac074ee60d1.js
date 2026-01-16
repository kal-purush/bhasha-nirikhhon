import { Component } from '@angular/core';
import { AuthService } from "../../auth/auth.service";
import { GameListServices } from "../game-list/game-list.services";
export var ArenaComponent = (function () {
    function ArenaComponent(userService, gameListSevices) {
        this.userService = userService;
        this.gameListSevices = gameListSevices;
    }
    ArenaComponent.prototype.ngOnInit = function () {
        var _this = this;
        this.userService.getUser()
            .subscribe(function (user) {
            _this.userName = user;
            console.log(user);
        });
        this.isUserPlaying = false;
        this.userPlayingToFalse();
        this.gameListSevices.userPlayingChosen$
            .subscribe(function (isPlaying) {
            _this.isUserPlaying = isPlaying;
            console.log(isPlaying);
        });
        this.gameListSevices.ArenaChosen
            .subscribe(function (isPlaying) {
            _this.arenas = isPlaying;
            console.log(isPlaying);
        });
    };
    ArenaComponent.prototype.userPlayingToFalse = function () {
        this.gameListSevices.setUserPlaying(false);
    };
    ArenaComponent.decorators = [
        { type: Component, args: [{
                    selector: 'my-arena',
                    template: "<div class=\"row\" *ngIf=\"isUserPlaying==false\">\n\n    <div class=\"col-md-5\" >\n        <h1>{{userName}}</h1>\n        <opponent-find></opponent-find>\n    </div>\n    <div class=\"col-md-7\">\n        <game-list></game-list>\n    </div>\n</div>\n   <div class=\"row\" *ngIf=\"isUserPlaying==true\">\n        <div class=\"col-md-5\" >\n        <arena-playing [arenas]=\"arenas\"></arena-playing>\n        </div>\n    </div>\n\n\n"
                },] },
    ];
    /** @nocollapse */
    ArenaComponent.ctorParameters = [
        { type: AuthService, },
        { type: GameListServices, },
    ];
    return ArenaComponent;
}());