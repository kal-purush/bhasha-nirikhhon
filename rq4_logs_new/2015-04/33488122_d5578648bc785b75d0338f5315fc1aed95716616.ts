/// <reference path="../../scripts/typings/all.d.ts" />

var expect = chai.expect;
describe('app.services.TodoService', (): void => {

    var $rootScope: ng.IRootScopeService;
    var localStorage: any;
    var todo: app.todo.ITodo;
    var todoService: app.services.ITodoService;

    beforeEach((): void => {
        angular.mock.module('app.services', ($provide) => {
            $provide.service('localStorage', () => {
                localStorage = {};
                localStorage.getItem = () => {};
                localStorage.setItem = () => {};
                return localStorage;
            });
        });
        angular.mock.inject([
            'app.services.TodoService',
            '$rootScope',
            (_todoService, _$rootScope) => {
                todoService = _todoService;
                $rootScope = _$rootScope;
            }
        ]);
        todo = new app.todo.Todo();
    });

    it('can be instantiated', () => {
        expect(todoService).to.not.equal(null);
    });

    describe('save()', () => {

        it('adds a todo to the list of todos', (done) => {
            todoService.save(todo)
                .then(() => {
                    return todoService.getAll();
                })
                .then((result) => {
                    expect(result.length).to.equal(1);
                    done();
                });
            $rootScope.$apply();
        });

        it('creates a new todo', (done) => {
            todoService.save(todo)
                .then((result) => {
                    expect(result.id).to.not.equal(null);
                    expect(result.id).to.be.greaterThan(0);
                    done();
                });
            $rootScope.$apply();
        });

    });

});