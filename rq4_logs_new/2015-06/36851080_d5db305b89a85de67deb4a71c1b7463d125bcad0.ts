/// <reference path="../reference/jasmine/jasmine.d.ts" />

import Square from '../src/Square';

describe('Square', function () {

    it('should calculate rectangle area', function () {
        var square = new Square(5),
            area = square.getArea();

        expect(area).toEqual(25);
    });

});