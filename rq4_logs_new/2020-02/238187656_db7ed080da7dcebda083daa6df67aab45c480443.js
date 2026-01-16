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
const core_1 = require("@angular/core");
const moment = require("moment");
const Proccessor = require("./processor/processor.facade");
const form_model_1 = require("./form.model");
const search_model_1 = require("./search.model");
let AppComponent = class AppComponent {
    constructor() {
        this.dateFormat = 'YYYY-MM-DD';
        this.serverErrorMsg = '';
        /**
         * 全部的搜尋標的大容器
         */
        this.searchTargets = new search_model_1.SearchTargets();
        /**
         * 當前的搜尋標的
         */
        this.currentSearchTarget = null;
        /**
         * 訂房網顯示區塊
         */
        this.$viewer = null;
        this.form = new form_model_1.Form();
        this.searchResult = [];
        let cityList = this.form.cityList;
        this.searchTargets.targets.forEach(target => {
            target.citys.forEach((value, cityName) => {
                if (cityList.indexOf(cityName) < 0) {
                    cityList.push(cityName);
                }
            });
        });
    }
    ;
    ngAfterViewInit() {
        $('input[name="daterange"]').daterangepicker({
            locale: {
                format: this.dateFormat
            },
            startDate: this.form.startDate,
            endDate: this.form.endDate,
            minDate: moment().format(this.dateFormat)
        }, (start, end, label) => {
            this.form.startDate = start;
            this.form.endDate = end;
        });
        let $viewer = this.$viewer = $('#viewer');
        $viewer.on("load", this.$viewerLoaded.bind(this));
    }
    ;
    /**
     * 驅動畫面重載的事件
     */
    reloadEvevtHandler() {
        location.reload();
    }
    ;
    /**
     * 驅動搜尋的事件
     */
    searchEvevtHandler() {
        this.searchTargets = new search_model_1.SearchTargets();
        this.searchResult = [];
        this.startANewSearch();
    }
    ;
    /**
     * 開工囉
     */
    startANewSearch() {
        let { searchTargets, $viewer } = this;
        // 設定搜尋目標為下一個
        let currentSearchTarget = this.currentSearchTarget = searchTargets.next;
        //沒下一家了
        if (!currentSearchTarget) {
            return;
        }
        ;
        //取得連結
        let src = currentSearchTarget.getSrc(this.form);
        //這家無法取得連結
        if (!src) {
            currentSearchTarget.searchState = search_model_1.SearchState.Finished;
            this.startANewSearch();
            return;
        }
        //設定連結
        $viewer[0].src = src;
    }
    ;
    /**
     * $viewer(訂房網區塊)下載完了
     */
    $viewerLoaded() {
        // 設定搜尋目標
        let currentSearchTarget = this.currentSearchTarget;
        //currentSearchTarget = (
        //    currentSearchTarget.searchState === SearchState.Finished
        //) ? this.searchTargets.next : currentSearchTarget;
        if (!currentSearchTarget)
            return;
        // 尋找搜尋方法
        let searchProccessor = Proccessor[currentSearchTarget.name];
        if (!searchProccessor) {
            console.error(`no proccessor for [${currentSearchTarget.name}]!!`);
        }
        else {
            const subject = searchProccessor(this.$viewer, currentSearchTarget);
            subject.subscribe((searchResult) => {
                this.searchResult = this.searchResult.concat(searchResult);
                // 結束了就換下一家
                if (currentSearchTarget.searchState === search_model_1.SearchState.Finished) {
                    subject.unsubscribe();
                    this.startANewSearch();
                }
            });
        }
    }
    ;
};
AppComponent = __decorate([
    core_1.Component({
        selector: 'app',
        templateUrl: 'app/app.html',
    }),
    __metadata("design:paramtypes", [])
], AppComponent);
exports.AppComponent = AppComponent;
;
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiYXBwLmNvbXBvbmVudC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbImFwcC5jb21wb25lbnQudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7OztBQUFBLHdDQUF5RDtBQUV6RCxpQ0FBaUM7QUFJakMsMkRBQTJEO0FBQzNELDZDQUFvQztBQUNwQyxpREFBdUY7QUFPdkYsSUFBYSxZQUFZLEdBQXpCO0lBMEJJO1FBeEJBLGVBQVUsR0FBRyxZQUFZLENBQUM7UUFDMUIsbUJBQWMsR0FBRyxFQUFFLENBQUM7UUFHcEI7O1dBRUc7UUFDSCxrQkFBYSxHQUFrQixJQUFJLDRCQUFhLEVBQUUsQ0FBQztRQUVuRDs7V0FFRztRQUNLLHdCQUFtQixHQUFpQixJQUFJLENBQUM7UUFFakQ7O1dBRUc7UUFDSCxZQUFPLEdBQVEsSUFBSSxDQUFDO1FBRXBCLFNBQUksR0FBUyxJQUFJLGlCQUFJLEVBQUUsQ0FBQztRQUV4QixpQkFBWSxHQUFtQixFQUFFLENBQUM7UUFJOUIsSUFBSSxRQUFRLEdBQUcsSUFBSSxDQUFDLElBQUksQ0FBQyxRQUFRLENBQUM7UUFDbEMsSUFBSSxDQUFDLGFBQWEsQ0FBQyxPQUFPLENBQUMsT0FBTyxDQUFDLE1BQU07WUFDckMsTUFBTSxDQUFDLEtBQUssQ0FBQyxPQUFPLENBQUMsQ0FBQyxLQUFLLEVBQUUsUUFBUTtnQkFDakMsRUFBRSxDQUFDLENBQUMsUUFBUSxDQUFDLE9BQU8sQ0FBQyxRQUFRLENBQUMsR0FBRyxDQUFDLENBQUMsQ0FBQyxDQUFDO29CQUNqQyxRQUFRLENBQUMsSUFBSSxDQUFDLFFBQVEsQ0FBQyxDQUFDO2dCQUM1QixDQUFDO1lBQ0wsQ0FBQyxDQUFDLENBQUM7UUFFUCxDQUFDLENBQUMsQ0FBQztJQUNQLENBQUM7SUFBQSxDQUFDO0lBRUYsZUFBZTtRQUNYLENBQUMsQ0FBQyx5QkFBeUIsQ0FBQyxDQUFDLGVBQWUsQ0FDeEM7WUFDSSxNQUFNLEVBQUU7Z0JBQ0osTUFBTSxFQUFFLElBQUksQ0FBQyxV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