// import {expect} from 'chai';
import 'mocha';
import * as sinon from 'sinon';
import { expect } from 'chai';

import * as child_process from 'child_process';

let exec: sinon.SinonStub;

import {ExecutorHelper} from '../helpers';
import {ApplicationModel} from '../models';


describe('ExecutorHelper', function() {
    const application = new ApplicationModel();
    ExecutorHelper.application = application;

    beforeEach(function() {
        exec = sinon.stub(child_process, 'exec').callsFake((_arg, callback) => {
            callback(undefined, {
                stdout: 'foo',
                stderr: 'bar',
            });
        });
    });

    afterEach(function() {
        exec.restore();
    });

    it('should execute', async function() {
        const returnValue = await ExecutorHelper.run('ssh-host', 'foobar', 'host');
        expect(returnValue).to.be.an('object');
        expect(returnValue).to.have.property('returnCode', 0);
        expect(returnValue).to.have.property('stdout', 'foo');
        expect(returnValue).to.have.property('stderr', 'bar');
        expect(exec.callCount).to.be.equal(1);
    });
});