///<reference path='./typings/tsd'/>

/**
 * postcss-mm
 *
 * Copyright, 2015 - furny.io
 *
 */

/**
 *
 * @author André König <andre.koenig@posteo.de>
 *
 */

var postcss = require('postcss');
var expect  = require('chai').expect;

var pkg = require('./package.json');

var processor = require('.');

var test = (input: string, output: string, opts: Object, done: Function) => {
    postcss([processor(opts)]).process(input).then((result) => {
        expect(result.css).to.eql(output);
        expect(result.warnings()).to.be.empty;
        done();
    }).catch((error) => {
        done(error);
    });
};

describe(pkg.name, () => {

    it('should convert with default values', (done) => {
        test('.foo{margin: 5mm}', '.foo{margin: 14px}', {}, done);
    });

    it('should convert with a custom dpi value', (done) => {
        test('.bar{margin: 5mm}', '.bar{margin: 59px}', {dpi: 300}, done);
    });
    
    it('should be able to not round the values', (done) => {
        test('.bar{margin: 5mm}', '.bar{margin: 59.05511811023622px}', {dpi: 300, round: false}, done);        
    });

});