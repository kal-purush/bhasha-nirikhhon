var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var __metadata = (this && this.__metadata) || function (k, v) {
    if (typeof Reflect === "object" && typeof Reflect.metadata === "function") return Reflect.metadata(k, v);
};
import { Component } from '@angular/core';
import { DataService } from '../../services/data.service';
var AirPathComponent = /** @class */ (function () {
    function AirPathComponent(dataService) {
        this.dataService = dataService;
        /**Array of airline info. */
        this.airlineInfo = [];
        /**Array of airport outgoing routes. */
        this.airportOutgoingRoutes = [];
        /**Array of airport info. */
        this.airportInfo = [];
        /**Array of paths. */
        this.airportPath = [];
    }
    AirPathComponent.prototype.ngOnInit = function () {
    };
    /**
     * Gets airline info from api.
     * @param alias Airport alias.
     */
    AirPathComponent.prototype.getAirlineInfo = function (alias) {
        var _this = this;
        this.dataService.getAirlineInfo(alias)
            .subscribe(function (res) {
            if (res) {
                _this.airlineInfo = res.data;
            }
        });
    };
    /**
     * Gets airport outgoing routes from api.
    * @param airport Airport alias.
    */
    AirPathComponent.prototype.getAirportOutgoing = function (airport) {
        var _this = this;
        this.dataService.getAirportOutgoing(airport)
            .subscribe(function (res) {
            if (res) {
                _this.airportOutgoingRoutes = res.data;
            }
        });
    };
    /**
    * Gets airport info from api.
    * @param pattern Pattern.
    */
    AirPathComponent.prototype.getAirportInfo = function (pattern) {
        var _this = this;
        this.dataService.getAirportInfo(pattern)
            .subscribe(function (res) {
            if (res) {
                _this.airportInfo = res.data;
            }
        });
    };
    /**
     * Gets path.
     * @param sourceAirport Source airport.
     * @param destinationAirport Destination airport.
     */
    AirPathComponent.prototype.getPath = function (sourceAirport, destinationAirport) {
        var _this = this;
        this.dataService.findPath(sourceAirport, destinationAirport)
            .subscribe(function (res) {
            if (res) {
                _this.airportPath = res.data;
            }
        });
    };
    AirPathComponent = __decorate([
        Component({
            styleUrls: ['./air-path.component.css'],
            templateUrl: './air-path.component.html',
        }),
        __metadata("design:paramtypes", [DataService])
    ], AirPathComponent);
    return AirPathComponent;
}());
export { AirPathComponent };
//# sourceMappingURL=air-path.component.js.map