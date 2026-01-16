// import {expect} from 'chai';
import 'mocha';
import * as sinon from 'sinon';
import {
    expect,
    assert,
} from 'chai';

import {
    ConfigModel,
} from '../models';

import {
    JSONSchemaNotFoundError,
} from '../errors';

import * as fse from 'fs-extra';


describe.only('ExecutorHelper', function() {
    // const application = new ApplicationModel();

    describe('getConfigSchema', function() {
        /**
         * Checks a JSON-Schema object
         */
        // tslint:disable-next-line:no-any
        function checkSchema(schema: any) {
            expect(schema).to.be.an('object');
            expect(schema).to.have.property('type', 'object');
            expect(schema.properties).to.be.an('object');
            expect(Object.keys(schema).length).to.be.equal(5);
            expect(schema.required).to.be.an('array');
            expect(schema.required).to.have.property('length', 4);
        }

        // tslint:disable-next-line:no-any
        let schema: any;
        it('should generate a schema with access to the interface', async function() {
            schema = await ConfigModel.getConfigSchema();

            checkSchema(schema);
        });

        it('should generate a schema without access to the interface', async function() {
            const pathExistsStub = sinon.stub(fse, 'pathExists');
            pathExistsStub.callsFake(async (path: string) => {
                return path.endsWith('config.schema.json');
            });
            const readFileStub = sinon.stub(fse, 'readFile');
            readFileStub.callsFake(async() => JSON.stringify(schema));

            const _schema = await ConfigModel.getConfigSchema();

            checkSchema(_schema);
            pathExistsStub.restore();
            readFileStub.restore();
        });

        it('should throw an error if no schema could be found', async function() {
            const pathExistsStub = sinon.stub(fse, 'pathExists');
            pathExistsStub.callsFake(async () => {
                return false;
            });

            // TODO This should be converted to https://github.com/domenic/chai-as-promised
            try {
                await ConfigModel.getConfigSchema();
            } catch (error) {
                expect(error).to.be.instanceOf(JSONSchemaNotFoundError);
                return;
            }
            assert(false, 'This should not be reached');
        });
    });
});