/// <reference path="../../../src/scripts/vendor_def/angularjs/angular.d.ts" />
/// <reference path="../../../src/scripts/vendor_def/angularjs/angular-mocks.d.ts" />
/// <reference path="../../../src/scripts/services/common/routes.ts" />
/// <reference path="../../../src/scripts/stanby-app.ts" />

beforeEach(function() {
    module('stanby');
});

describe('Test filters', function() {

    /**
     * To use a Filter in code (controller, services, directives, test), simply append "Filter"
     * to the end of the filter name.
     *
     * eg, we have a filter named "calculateAge" in the stanby module. So we inject
     * "calculateAgeFilter to use it
     *
     * For more info see: https://docs.angularjs.org/guide/filter
     */
    it ('test age filter', inject(function(calculateAgeFilter) {
        var date = new Date();
        date.setFullYear(date.getFullYear() - 20);

        var bday: st.response.masters.YearMonthDay = {
            year: date.getFullYear(), month: date.getMonth(), day: date.getDate()
        };

        expect(calculateAgeFilter(bday)).toEqual(20);
    }));

    it ('should format the date', inject(function(formatYearMonthDayFilter) {
        var bday: st.response.masters.YearMonthDay = {
            year: 2000, month: 1, day: 31
        }

        expect(formatYearMonthDayFilter(bday)).toEqual("2000年1月31日");
    }));

});
