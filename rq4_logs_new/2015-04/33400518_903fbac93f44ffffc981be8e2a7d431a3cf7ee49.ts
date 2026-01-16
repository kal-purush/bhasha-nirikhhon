/// <reference path="../typings/test/tsd.d.ts" />

import CFG = require('../lib/cfg');
import chai = require('chai');
var expect = chai.expect;

describe('CFG Tests:', () => {

    describe('CFG', () => {
        it('can be constructed empty', () => {
            let cfg = new CFG.CFG();
            expect(cfg.startNode).to.be.undefined;
        });
        it('can have basic blocks added', () => {
            let cfg = new CFG.CFG();
            let bb: CFG.BasicBlock = cfg.createNode('bb.entry', 0);
            expect(cfg.startNode).to.equal(bb);
            expect(cfg.basicBlockMap.get(bb.id)).to.equal(bb);
        });
    });

    describe('BasicBlockEdge', () => {
        it('can be created correctly', () => {
            let cfg = new CFG.CFG();
            let bb1 = cfg.createNode('bb.entry', 0);
            let bb2 = cfg.createNode('bb.exit', 1);
            let edge = new CFG.BasicBlockEdge(cfg, bb1, bb2);
            expect(cfg.edgeList[0]).to.equal(edge);
            expect(cfg.getSrc(edge)).to.equal(bb1);
            expect(cfg.getDst(edge)).to.equal(bb2);
        });
    });

});