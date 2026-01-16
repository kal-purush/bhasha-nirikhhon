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
var ApprovalnModalComponent = (function () {
    function ApprovalnModalComponent() {
        this.isOpen = false;
        //        {id: 1, name: 'Concerta', totalDailyAmount:4 , pillTakenToday: 1, dosage: 2, frequencyAmount: 1, frequency: 'Daily'},
        this.presciptionModal = { id: 0, rxname: "e", totalDailyAmount: 0, pillTakenToday: 0, dosage: 0, frequencyAmount: 0, frequency: '' };
        this.confirm = new core_1.EventEmitter();
    }
    ApprovalnModalComponent.prototype.open = function (prescription) {
        /*
        this.presciptionModal = {
            id: 4,
            rxname: prescription.rxname,
            totalDailyAmount: prescription.totalDailyAmount,
            pillTakenToday: prescription.pillTakenToday,
            dosage: prescription.dosage,
            frequencyAmount: prescription.frequencyAmount,
            frequency: prescription.frequency,
        };
        */
        this.isOpen = true;
    };
    ApprovalnModalComponent.prototype.ok = function () {
        this.isOpen = false;
        this.confirm.emit(this.presciptionModal);
    };
    ApprovalnModalComponent.prototype.cancel = function () {
        this.isOpen = false;
    };
    __decorate([
        core_1.Output(), 
        __metadata('design:type', core_1.EventEmitter)
    ], ApprovalnModalComponent.prototype, "confirm", void 0);
    ApprovalnModalComponent = __decorate([
        core_1.Component({
            selector: 'confirmModal',
            template: "\n\n          <div #dialog class=\"md-dialog mdl-color--blue mdl-shadow--2dp\" [hidden]=\"!isOpen\" (keydown.esc)=\"cancel()\" (keydown.enter)=\"ok()\">\n\n            <div class=\"md-dialog-content\">\n                <div class=\"typo-styles__demo md-header\">\n                    <ng-content select=\"[title]\"></ng-content>\n                </div>\n                <div class=\"md-dialog-content-body\">\n                    <ng-content select=\"[content]\"></ng-content>\n                </div>\n            </div>\n            <div class=\"md-dialog-actions\">\n                <button class=\"mdl-button mdl-js-button mdl-js-ripple-effect\" (click)=\"ok()\">\n                    Take\n                </button>\n                <button class=\"mdl-button mdl-js-button mdl-js-ripple-effect\" (click)=\"cancel()\">\n                    Cancel\n                </button>\n            </div>\n            <div tabindex=\"0\" (focus)=\"dialog.focus()\"></div>\n        </div>\n        <div class=\"md-backdrop\" [hidden]=\"!isOpen\"></div>\n    ",
            styles: [
                "\n        .md-dialog {\n            position: fixed;\n            top: 50%;\n            left: 50%;\n            transform: translate(-50%, -50%);\n            margin: 0 auto;\n            z-index: 51;\n            width: 600px;\n            max-width: 50%;\n            height: 200px;\n            max-height: 100%;\n            background: white;\n        }\n\n        .md-dialog-content {\n            color: darkblue;\n            font-sizing: 50em;\n            font-weight: 20em;\n            min-width: 450px;\n            min-height: 100px;\n            padding: 24px;\n            -webkit-order: 1;\n            -ms-flex-order: 1;\n            order: 1;\n            -webkit-flex-direction: column;\n            -ms-flex-direction: column;\n            flex-direction: column;\n            overflow: auto;\n            -webkit-overflow-scrolling: touch;\n        }\n\n        .md-dialog-content > mdl-typography--headline {\n            font-weight: 600;\n        }\n\n        .md-dialog-content-body {\n            padding: 15px 0 5px 0;\n        }\n\n        .md-header {\n            background: #428bca;\n            display: -webkit-flex;\n            display: -ms-flexbox;\n            display: flex;\n            -webkit-order: 2;\n            -ms-flex-order: 2;\n            order: 2;\n            box-sizing: border-box;\n            -webkit-align-items: center;\n            -ms-flex-align: center;\n            align-items: center;\n            -webkit-justify-content: flex-end;\n            -ms-flex-pack: end;\n            justify-content: center;\n            margin-bottom: 0;\n            \n  \n            min-height: 25px;\n            overflow: hidden;\n        }\n\n        .md-dialog-actions {\n            background: #428bca;\n            display: -webkit-flex;\n            display: -ms-flexbox;\n            display: flex;\n            -webkit-order: 2;\n            -ms-flex-order: 2;\n            order: 2;\n            box-sizing: border-box;\n            -webkit-align-items: center;\n            -ms-flex-align: center;\n            align-items: center;\n            -webkit-justify-content: flex-end;\n            -ms-flex-pack: end;\n            justify-content: flex-end;\n            margin-bottom: 0;\n            padding-right: 8px;\n            padding-left: 16px;\n            min-height: 52px;\n            overflow: hidden;\n        }\n\n        .md-backdrop {\n            background-color: rgba(229,222,222,4.0);\n            opacity: .48;\n            transition: opacity 450ms;\n            position: fixed;\n            top: 0;\n            bottom: 0;\n            left: 0;\n            right: 0;\n            z-index: 50;\n        }\n        "
            ]
        }), 
        __metadata('design:paramtypes', [])
    ], ApprovalnModalComponent);
    return ApprovalnModalComponent;
}());
exports.ApprovalnModalComponent = ApprovalnModalComponent;
//# sourceMappingURL=approval.modal.component.js.map