/**
 * Created by Salifukayta on 09/02/2016.
 */


describe('CitiesController test', function(){
    var scope;
    var citiesCtrl;
    var stateMock;
    var cityPassServiceMock;
    var localstorageMock;
    var citiesServiceMock;
    var gettextCatalogMock;
    var citiesServiceDeferred;

    // load the controller's module
    beforeEach(module('cloudPrecipitation'));

    // disable template caching
    beforeEach(module(function($provide, $urlRouterProvider) {
        $provide.value('$ionicTemplateCache', function(){} );
        $urlRouterProvider.deferIntercept();
    }));

    // instantiate the controller and mocks for every test
    beforeEach(inject(function($rootScope, $q, $controller) {
        scope = $rootScope.$new();
        citiesServiceDeferred = $q.defer();

        localstorageMock = {
            getObject: jasmine.createSpy('$localstorage spy getObject')
                .and.returnValue({paris: {name: "paris"}}),
            get: jasmine.createSpy('$localstorage spy get')
                .and.returnValue({paris: {name: "paris"}}),
        }

        // mock $state
        stateMock = jasmine.createSpyObj('$state spy', ['go']);

        cityPassServiceMock = jasmine.createSpyObj('cityPassService spy', ['set']);

        citiesServiceMock = {
            search: jasmine.createSpy('citiesServiceMock spy search')
                .and.returnValue(citiesServiceDeferred.promise),
        }

        gettextCatalogMock = jasmine.createSpyObj('gettextCatalogMock spy', ['getString']);

        citiesCtrl = $controller('CitiesController', {
            $scope: scope, $state: stateMock, $localstorage: localstorageMock, citiesService: citiesServiceMock,
            cityPassService: cityPassServiceMock, gettextCatalog: gettextCatalogMock});
    }));

    describe('#goTo', function() {

        it('should call state.go and cityPassService.set', function () {
            var cityMock = {name: "paris"};
            var hasNoFavoriteCitiesResult = citiesCtrl.goTo(cityMock);
            expect(cityPassServiceMock.set).toHaveBeenCalledWith(cityMock);
            expect(stateMock.go).toHaveBeenCalledWith('app.radar', ({'useGeoloc': false}));
        });
    });

    describe('#search no cities', function() {

        it('should found none city', function () {
            citiesCtrl.cityToSearch = "";
            citiesCtrl.search();
            expect(citiesCtrl.cities).toEqual([]);
        });
    });

    describe('#search some cities', function() {

        beforeEach(inject(function(_$rootScope_)  {
            $rootScope = _$rootScope_;
            citiesCtrl.cityToSearch = "paris";
            citiesCtrl.search();
        }));

        it('should found the city paris success', function () {
            var cityMock = {paris: {name: "paris"}};
            citiesServiceDeferred.resolve(cityMock);
            $rootScope.$digest();
            expect(citiesCtrl.error).toBeNull();
            expect(citiesCtrl.cities).toEqual(cityMock);
        });

        it('should found the city paris fail', function () {
            var errorMock = "mock error";
            citiesServiceDeferred.reject(errorMock);
            $rootScope.$digest();
            expect(citiesCtrl.error).toEqual(errorMock);
            expect(citiesCtrl.cities).toEqual([]);
        });
    });

    describe('clearSearch', function() {

        it('should the cityToSearch to be empty string', function() {
            citiesCtrl.cityToSearch = "paris";
            citiesCtrl.clearSearch();
            expect(citiesCtrl.cityToSearch).toEqual("");
        });
    });

    describe('#refreshCitiesList without cityToSearch', function() {

        beforeEach(inject(function($controller) {
            localstorageMock = {
                getObject: jasmine.createSpy('$localstorage spy getObject')
                    .and.returnValue({}),
                get: jasmine.createSpy('$localstorage spy get')
                    .and.returnValue(null),
            };

            citiesCtrl = $controller('CitiesController', {
                $scope: scope, $state: stateMock, $localstorage: localstorageMock, citiesService: citiesServiceMock,
                cityPassService: cityPassServiceMock, gettextCatalog: gettextCatalogMock});
        }));

        it('should cityToSearch === " " and call search() and clearSearch', function() {
            spyOn(citiesCtrl, "search");
            spyOn(citiesCtrl, "clearSearch");
            citiesCtrl.refreshCitiesList();
            expect(citiesCtrl.cityToSearch).toEqual(" ");
            expect(citiesCtrl.search).toHaveBeenCalled();
            expect(citiesCtrl.clearSearch).toHaveBeenCalled();
        });
    });

    describe('#refreshCitiesList with cityToSearch', function() {
        beforeEach(inject(function($controller) {
            localstorageMock = {
                getObject: jasmine.createSpy('$localstorage spy getObject')
                    .and.returnValue({name: "paris"}),
                get: jasmine.createSpy('$localstorage spy get')
                    .and.returnValue({name: "paris"}),
            };

            citiesCtrl = $controller('CitiesController', {
                $scope: scope, $state: stateMock, $localstorage: localstorageMock, citiesService: citiesServiceMock,
                cityPassService: cityPassServiceMock, gettextCatalog: gettextCatalogMock});
        }));

        it('should cityToSearch === "paris" and call search() and clearSearch', function() {
            spyOn(citiesCtrl, "search");
            spyOn(citiesCtrl, "clearSearch");
            citiesCtrl.refreshCitiesList();
            expect(citiesCtrl.cityToSearch).toEqual("paris");
            expect(citiesCtrl.search).toHaveBeenCalled();
            expect(citiesCtrl.clearSearch).toHaveBeenCalled();
        });
    });

});