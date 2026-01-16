/// <reference path="../../../src/scripts/vendor_def/angularjs/angular.d.ts" />
/// <reference path="../../../src/scripts/vendor_def/angularjs/angular-mocks.d.ts" />
/// <reference path="../../../src/scripts/services/common/routes.ts" />
/// <reference path="../../../src/scripts/stanby-app.ts" />
/// <reference path="../../../src/scripts/services/users/user-accounts-service.ts" />
describe('User User Accounts Service', function() {

    beforeEach(function() {
        module('stanby');
    });

    var usersAccountService;
    var $q;
    var $httpBackend;
    var $rootScope;

    // Example of how to mock an entire object. In this case a route factory.
    // Note: mocking the entire object may require lots of code, especially since our
    // classes are quite large
    //beforeEach(function() {
    // To mock an entire object:
    //routesMocked = {
    //    validation: {
    //        emailDuplicate: function(email) {
    //            console.log("Checking duplicate email: " + email);
    //            return false;
    //        }
    //    },
    //    configuration: {
    //        retrieve: function() {
    //            //
    //            //var defer = $q.defer();
    //            //defer.resolve("done");
    //            //return defer.promise;
    //
    //            var promise = { then: jasmine.createSpy() };
    //            return promise;
    //        }
    //    }
    //    // ... this only mocks 2 functions in the routes service. If other functions
    //    // are called, an error will be thrown
    //};
    // // perform the mock via $provide
    //module(function($provide) {
    //    $provide.value('routes', routesMocked);
    //});
    //});

    /**
     * The inject function is from angular-mocks (ngMock) used to resolve references.
     * See https://docs.angularjs.org/api/ngMock/function/angular.mock.inject
     */
    beforeEach(inject(function(routes, $q, $httpBackend) {

        // Here we are overriding the routes.validation.emailDuplicate function to
        // create a mock function.
        // Alternatively you can mock the $http calls, (which we do below), but
        // this gives us flexibility for this test case.
        routes.validation.emailDuplicate = function(data) {
            // emailDuplicate() returns a promise, so create a promise using $q
            var deferred = $q.defer();

            if (data === 'test@test.com') {
                deferred.resolve("Email is ok");
            }
            else {
                deferred.reject("duplicate email");
            }

            return deferred.promise;
        }

        /**
         * Mocking the create function as well. If this is not mocked, the test
         * will never resolve as create() also returns a promise after validateEmail
         * succeeds
         */
        routes.users.create = function(user) {
            var deferred = $q.defer();
            // no particular test here, just resolve the promise
            deferred.resolve("created");

            return deferred.promise;
        }
    }));

    /**
     * Set up variables for local access as well as mocking $http GET and POST calls
     *
     * usersAccountService: is the name of a service within the stanby module
     */
    beforeEach(inject(function(_usersAccountService_, _$rootScope_, _$q_, _$httpBackend_) {
        // ngMock returns references to these services, the underscore notation is
        // an angular concept, it automatically strips the underscores when it
        // searches for the services
        usersAccountService = _usersAccountService_;
        $rootScope = _$rootScope_;
        $q = _$q_;
        $httpBackend = _$httpBackend_;

        // mock http responses. Not testing these at the moment.
        $httpBackend.whenGET('/api/configuration').respond(200, {});
        $httpBackend.whenGET('/api/account').respond(200, {
            account: {
                roles: []
            }
        });
        $httpBackend.whenPOST('/api/corporate/users').respond(200, {});
        $httpBackend.whenGET('/templates/users/list.html').respond(200, {});

    }));

    it ('User Account Service should test for duplicate emails when creating a User', function() {
        var user = {
            email: 'test@test.com',
            currentEmail: '',
            fullName: 'Test User',
            password: 'password',
            status: 'OK',
            title: '',
            versionNo: 1,
            roles: ['ADMIN'],
            roleVersionNo: 1
        };

        var emailIsOk;

        var promise = usersAccountService.create(user);

        promise.then((good) => {
            emailIsOk = true;
        }, (bad) => {
            emailIsOk = false;
        });

        // required to flush promises
        $rootScope.$digest();

        expect(emailIsOk).toEqual(true);
    });


    it ('User Account Service should test for duplicate emails when creating a User', function() {
        var user = {
            email: 'error@test.com',
            currentEmail: '',
            fullName: 'Test User',
            password: 'password',
            status: 'OK',
            title: '',
            versionNo: 1,
            roles: ['ADMIN'],
            roleVersionNo: 1
        };

        var emailIsOk;

        var promise = usersAccountService.create(user);

        promise.then((good) => {
            emailIsOk = true;
        }, (bad) => {
            emailIsOk = false;
        });

        // required to flush promises
        $rootScope.$digest();

        expect(emailIsOk).toEqual(false);
    });

});