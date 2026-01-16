"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var core_1 = require('@angular/core');
var dashboard_service_1 = require('../shared/dashboard.service');
var DashboardsManagerComponent = (function () {
    function DashboardsManagerComponent(dashboardService, router, identityService) {
        this.dashboardService = dashboardService;
        this.router = router;
        this.identityService = identityService;
        this.dashboards = [];
        this.userIsAuthenticated = this.identityService.isAuthenticated;
    }
    DashboardsManagerComponent.prototype.ngOnInit = function () {
        var _this = this;
        this.identityService.getUserIsAuthenticated().subscribe(function (isUserAuthenticated) {
            _this.userIsAuthenticated = isUserAuthenticated;
        });
        this.maxDashboards = 18;
        this.dashboardService.getAll().subscribe(function (dashboards) {
            _this.dashboards = dashboards;
            while (_this.maxDashboards <= _this.dashboards.length) {
                _this.maxDashboards += 6;
            }
            _this.maxDashboardCount = new Array(_this.maxDashboards);
        });
    };
    DashboardsManagerComponent.prototype.goToDashboard = function (id) {
        this.router.navigate(['/dashboard', id]);
    };
    DashboardsManagerComponent.prototype.deleteDashboard = function (dashboard) {
        var _this = this;
        this.dashboardService.remove(dashboard.id).subscribe(function () {
            _this.deleteEmptyRow();
        });
    };
    DashboardsManagerComponent.prototype.createDashboard = function () {
        this.router.navigate(['/dashboard', 'new']);
    };
    DashboardsManagerComponent.prototype.deleteEmptyRow = function () {
        var difference = this.maxDashboards - this.dashboards.length;
        if (difference > 5 && this.maxDashboards > 18) {
            this.maxDashboards -= 6;
        }
    };
    DashboardsManagerComponent = __decorate([
        core_1.Component({
            selector: 'radiator-dashboards-manager',
            templateUrl: './dashboards-manager.component.html',
            styleUrls: ['./dashboards-manager.component.scss'],
            providers: [dashboard_service_1.DashboardService],
        })
    ], DashboardsManagerComponent);
    return DashboardsManagerComponent;
}());
exports.DashboardsManagerComponent = DashboardsManagerComponent;