webpackJsonp([0],{

/***/ 541:
/***/ (function(module, __webpack_exports__, __webpack_require__) {

"use strict";
Object.defineProperty(__webpack_exports__, "__esModule", { value: true });
/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, "ManageDocumentPageModule", function() { return ManageDocumentPageModule; });
/* harmony import */ var __WEBPACK_IMPORTED_MODULE_0__angular_core__ = __webpack_require__(0);
/* harmony import */ var __WEBPACK_IMPORTED_MODULE_1_ionic_angular__ = __webpack_require__(54);
/* harmony import */ var __WEBPACK_IMPORTED_MODULE_2__manage_document__ = __webpack_require__(542);
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};



var ManageDocumentPageModule = (function () {
    function ManageDocumentPageModule() {
    }
    ManageDocumentPageModule = __decorate([
        Object(__WEBPACK_IMPORTED_MODULE_0__angular_core__["I" /* NgModule */])({
            declarations: [
                __WEBPACK_IMPORTED_MODULE_2__manage_document__["a" /* ManageDocumentPage */],
            ],
            imports: [
                __WEBPACK_IMPORTED_MODULE_1_ionic_angular__["e" /* IonicPageModule */].forChild(__WEBPACK_IMPORTED_MODULE_2__manage_document__["a" /* ManageDocumentPage */]),
            ],
        })
    ], ManageDocumentPageModule);
    return ManageDocumentPageModule;
}());

//# sourceMappingURL=manage-document.module.js.map

/***/ }),

/***/ 542:
/***/ (function(module, __webpack_exports__, __webpack_require__) {

"use strict";
/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, "a", function() { return ManageDocumentPage; });
/* harmony import */ var __WEBPACK_IMPORTED_MODULE_0__angular_core__ = __webpack_require__(0);
/* harmony import */ var __WEBPACK_IMPORTED_MODULE_1__angular_forms__ = __webpack_require__(30);
/* harmony import */ var __WEBPACK_IMPORTED_MODULE_2_ionic_angular__ = __webpack_require__(54);
/* harmony import */ var __WEBPACK_IMPORTED_MODULE_3__providers_database_database__ = __webpack_require__(325);
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var __metadata = (this && this.__metadata) || function (k, v) {
    if (typeof Reflect === "object" && typeof Reflect.metadata === "function") return Reflect.metadata(k, v);
};




var ManageDocumentPage = (function () {
    function ManageDocumentPage(navCtrl, params, _FB, _DB, _ALERT) {
        this.navCtrl = navCtrl;
        this.params = params;
        this._FB = _FB;
        this._DB = _DB;
        this._ALERT = _ALERT;
        /**
         * @name city
         * @type {string}
         * @public
         * @description     Model for city form field
         */
        this.city = '';
        /**
         * @name population
         * @type {string}
         * @public
         * @description     Model for population form field
         */
        this.population = '';
        /**
         * @name established
         * @type {string}
         * @public
         * @description     Model for established form field
         */
        this.established = '';
        /**
         * @name docID
         * @type {string}
         * @public
         * @description     property that stores an edited document's ID
         */
        this.docID = '';
        /**
         * @name isEditable
         * @type {boolean}
         * @public
         * @description     property that stores value to signify whether
                            we are editing an existing document or not
         */
        this.isEditable = false;
        /**
         * @name title
         * @type {string}
         * @public
         * @description     property that defines the template title value
         */
        this.title = 'Add a new document';
        /**
         * @name _COLL
         * @type {string}
         * @private
         * @description     property that stores the value for the database collection
         */
        this._COLL = "Britain";
        // Use Formbuilder API to create a FormGroup object
        // that will be used to programmatically control the
        // form / form fields in the component template
        this.form = _FB.group({
            'city': ['', __WEBPACK_IMPORTED_MODULE_1__angular_forms__["f" /* Validators */].required],
            'population': ['', __WEBPACK_IMPORTED_MODULE_1__angular_forms__["f" /* Validators */].required],
            'established': ['', __WEBPACK_IMPORTED_MODULE_1__angular_forms__["f" /* Validators */].required]
        });
        // If we have navigation parameters then we need to
        // parse these as we know these will be used for
        // editing an existing record
        if (params.get('isEdited')) {
            var record = params.get('record');
            this.city = record.location.city;
            this.population = record.location.population;
            this.established = record.location.established;
            this.docID = record.location.id;
            this.isEditable = true;
            this.title = 'Update this document';
        }
    }
    /**
     * Saves form data as newly added/edited record within Firebase Realtime
     * database and handles uploading of media asset to Firebase Storage
     *
     * @public
     * @method saveDocument
     * @param  val          {any}              Form data
     * @return {none}
     */
    ManageDocumentPage.prototype.saveDocument = function (val) {
        var _this = this;
        var city = this.form.controls["city"].value, population = this.form.controls["population"].value, established = this.form.controls["established"].value;
        // If we are editing an existing record then handle this scenario
        if (this.isEditable) {
            // Call the DatabaseProvider service and pass/format the data for use
            // with the updateDocument method
            this._DB.updateDocument(this._COLL, this.docID, {
                city: city,
                population: population,
                established: established
            })
                .then(function (data) {
                _this.clearForm();
                _this.displayAlert('Success', 'The document ' + city + ' was successfully updated');
            })
                .catch(function (error) {
                _this.displayAlert('Updating document failed', error.message);
            });
        }
        else {
            // Call the DatabaseProvider service and pass/format the data for use
            // with the addDocument method
            this._DB.addDocument(this._COLL, {
                city: city,
                population: population,
                established: established
            })
                .then(function (data) {
                _this.clearForm();
                _this.displayAlert('Record added', 'The document ' + city + ' was successfully added');
            })
                .catch(function (error) {
                _this.displayAlert('Adding document failed', error.message);
            });
        }
    };
    /**
     * Provide feedback to user after an operation has succeeded/failed
     *
     * @public
     * @method displayAlert
     * @param  title          {String}           Heading for alert message
     * @param  message        {String}           Content for alert message
     * @return {none}
     */
    ManageDocumentPage.prototype.displayAlert = function (title, message) {
        var alert = this._ALERT.create({
            title: title,
            subTitle: message,
            buttons: ['Got it!']
        });
        alert.present();
    };
    /**
     * Clear all form data
     *
     * @public
     * @method clearForm
     * @return {none}
     */
    ManageDocumentPage.prototype.clearForm = function () {
        this.city = '';
        this.population = '';
        this.established = '';
    };
    ManageDocumentPage = __decorate([
        Object(__WEBPACK_IMPORTED_MODULE_0__angular_core__["m" /* Component */])({
            selector: 'page-manage-document',template:/*ion-inline-start:"/Users/kaaro/Desktop/src/ionic-3-firestore/src/pages/manage-document/manage-document.html"*/'<ion-header>\n\n  <ion-navbar>\n    <ion-title>{{ title }}</ion-title>\n  </ion-navbar>\n\n</ion-header>\n\n\n<ion-content padding>\n  <form\n    [formGroup]=\'form\'\n    (ngSubmit)=\'saveDocument(form.value)\'>\n\n    <ion-item>\n      <ion-label stacked>City:</ion-label>\n      <ion-input\n        type=\'text\'\n        formControlName=\'city\'\n        [(ngModel)]=\'city\'>\n      </ion-input>\n    </ion-item>\n\n    <ion-item>\n       <ion-label stacked>Population:</ion-label>\n       <ion-input\n          type="text"\n          formControlName="population"\n          [(ngModel)]="population"></ion-input>\n 	  </ion-item>\n\n    <ion-item>\n       <ion-label stacked>Established:</ion-label>\n       <ion-input\n          type="text"\n          formControlName="established"\n          [(ngModel)]="established"></ion-input>\n 	  </ion-item>\n\n 	  <ion-item>\n       <button\n         ion-button\n         block\n         color="primary"\n         text-center\n         padding-top\n         padding-bottom\n         [disabled]="!form.valid">\n          <div *ngIf="!isEditable">\n             Add a new document\n          </div>\n\n          <div *ngIf="isEditable">\n             Update this document\n          </div>\n          </button>\n 	  </ion-item>\n\n  </form>\n  \n</ion-content>\n'/*ion-inline-end:"/Users/kaaro/Desktop/src/ionic-3-firestore/src/pages/manage-document/manage-document.html"*/,
        }),
        __metadata("design:paramtypes", [__WEBPACK_IMPORTED_MODULE_2_ionic_angular__["f" /* NavController */],
            __WEBPACK_IMPORTED_MODULE_2_ionic_angular__["g" /* NavParams */],
            __WEBPACK_IMPORTED_MODULE_1__angular_forms__["a" /* FormBuilder */],
            __WEBPACK_IMPORTED_MODULE_3__providers_database_database__["a" /* DatabaseProvider */],
            __WEBPACK_IMPORTED_MODULE_2_ionic_angular__["a" /* AlertController */]])
    ], ManageDocumentPage);
    return ManageDocumentPage;
}());

//# sourceMappingURL=manage-document.js.map

/***/ })

});
//# sourceMappingURL=0.js.map