'use strict';

var proxyquire = require('proxyquire').noPreserveCache();

xdescribe('Thing API Router:', function () {

    var thingCtrlStub = {
        index: 'thingCtrl.index',
        show: 'thingCtrl.show',
        create: 'thingCtrl.create',
        update: 'thingCtrl.update',
        destroy: 'thingCtrl.destroy'
    };

    var routerStub = {
        get: function() { },
        post: function() {},
        delete: function() {},
        patch: function() {},
        put: function() {}
    };

    // require the index with our stubbed out modules
    var thingIndex = proxyquire('../../../server/api/thing/index.js', {
        'express': {
            Router: function () {
                return routerStub;
            }
        },
        './thing.controller': thingCtrlStub
    });

    beforeEach(function () {
        spyOn(routerStub, 'get');
        spyOn(routerStub, 'put');
        spyOn(routerStub, 'patch');
        spyOn(routerStub, 'post');
        spyOn(routerStub, 'delete');
    });

    it('should return an express router instance', function () {
        expect(thingIndex).toBe(routerStub);
    });

    describe('GET /api/things', function () {

        it('should route to thing.controller.index', function () {
            thingIndex.get('/');
            expect(routerStub.get).toHaveBeenCalledWith('/', 'thingCtrl.index');
        });

    });

    describe('GET /api/things/:id', function () {

        it('should route to thing.controller.show', function () {
            expect(routerStub.get.withArgs('/:id', 'thingCtrl.show')).toHaveBeenCalled();
        });

    });

    describe('POST /api/things', function () {

        it('should route to thing.controller.create', function () {
            expect(routerStub.post.withArgs('/', 'thingCtrl.create')).toHaveBeenCalled();
        });

    });

    describe('PUT /api/things/:id', function () {

        it('should route to thing.controller.update', function () {
            expect(routerStub.put.withArgs('/:id', 'thingCtrl.update')).toHaveBeenCalled();
        });

    });

    describe('PATCH /api/things/:id', function () {

        it('should route to thing.controller.update', function () {
            expect(routerStub.patch.withArgs('/:id', 'thingCtrl.update')).toHaveBeenCalled();
        });

    });

    describe('DELETE /api/things/:id', function () {

        it('should route to thing.controller.destroy', function () {
            expect(routerStub.delete.withArgs('/:id', 'thingCtrl.destroy')).toHaveBeenCalled();
        });

    });

});