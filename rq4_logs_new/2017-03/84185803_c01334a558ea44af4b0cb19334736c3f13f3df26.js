"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var __metadata = (this && this.__metadata) || function (k, v) {
    if (typeof Reflect === "object" && typeof Reflect.metadata === "function") return Reflect.metadata(k, v);
};
var core_1 = require('@angular/core');
var login_user_model_1 = require('./login.user.model');
var account_service_1 = require('./account.service');
var router_1 = require('@angular/router');
var LoginComponent = (function () {
    function LoginComponent(authService, router) {
        this.authService = authService;
        this.router = router;
        this.loginUser = new login_user_model_1.LoginUserModel('tester@tester.com', 'Abcde123');
        this.clientId = 'UMPB1q9XBKudOD58cyVYACOp22a5OjgY';
        this.token = '';
        this.error = "";
        this.isAuthenticated = false;
    }
    LoginComponent.prototype.submitLogin = function (form) {
        var _this = this;
        this.error = "";
        this.authService.login(this.loginUser, this.clientId, '/analytics').subscribe(function (response) {
            console.log(response);
            if (response.status == "fail") {
                console.log("FAIL");
                _this.error = response.error.error_message;
            }
            else {
                _this.token = response.data.access_token;
                if (_this.token.length != 0) {
                    console.log("SUCCESS length:" + _this.token.length);
                    localStorage.setItem("sms_access_token", _this.token);
                    _this.router.navigate(['/analytics']);
                    _this.authService.isAuthenticated = true;
                }
                console.log(_this.isAuthenticated);
            }
        }, function (err) {
            console.log("Returning Eror:" + err);
            if (err.error_message) {
                _this.error = err.error_message;
            }
        });
    };
    LoginComponent.prototype.submitLogout = function () {
        this.authService.logout('/welcome');
    };
    LoginComponent = __decorate([
        core_1.Component({
            selector: 'login',
            moduleId: module.id,
            templateUrl: './account-login.component.html',
        }), 
        __metadata('design:paramtypes', [account_service_1.AuthenticationService, router_1.Router])
    ], LoginComponent);
    return LoginComponent;
}());
exports.LoginComponent = LoginComponent;
//# sourceMappingURL=account-login.component.js.map