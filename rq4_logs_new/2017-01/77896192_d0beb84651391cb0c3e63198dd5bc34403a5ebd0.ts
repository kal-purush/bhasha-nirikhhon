/// <reference path="../typings/index.d.ts" />
import {expect} from 'chai';
import * as compy from '../src/virtualprocessor'

describe('virtual processor', () => {

    it('Should load data into registers', () => {
    
        var startingMemory = new compy.Memory([3, 1, 4, 1, 5, 8, 2, 6, 5, 3, 5]);
        var startingCpu = new compy.Processor(0, 0, 0, 0, startingMemory.data.length, startingMemory);
        var nextStepCpu = compy.loadRegister(startingCpu, "registerA", 1);
        expect(nextStepCpu.registerA).to.equal(1);
    })

});