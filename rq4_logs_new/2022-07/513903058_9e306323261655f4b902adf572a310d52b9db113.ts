import 'mocha';
import chai, { expect } from 'chai';
import promised from 'chai-as-promised';
import PapiService from '../papi.service';
import { CoreSchemaService } from '../coreSchema.service';
import { mockClient } from './consts';
import { Request } from "@pepperi-addons/debug-server";
import { PapiClient } from '@pepperi-addons/papi-sdk';
import { CoreService } from '../core.service';
import { UNIQUE_FIELDS } from '../constants';

chai.use(promised);

describe('GET resource by unique field', async () => {
    const requestedKey = 'dd51e0a9-83f3-49b5-9074-35f13916b340';
    const requestedUUID = 'dd51e0a9-83f3-49b5-9074-35f13916b340';
    const requestedInternalID = 11496119;
    const requestedExternalID = "MyExternalID";

    const papiClient = new PapiClient({
        baseURL: mockClient.BaseURL,
        token: mockClient.OAuthAccessToken,
        addonUUID: mockClient.AddonUUID,
        actionUUID: mockClient.ActionUUID,
    });

    papiClient.get = async (url: string) => {
            return Promise.resolve(
                JSON.parse('{"InternalID":11496119, "ExternalID":"MyExternalID", "UUID":"dd51e0a9-83f3-49b5-9074-35f13916b340","ExternalID":"","CreationDateTime":"2022-05-09T13:39:18Z","Email":"testing@testing.testing","FirstName":"aaa","Hidden":false,"IsInTradeShowMode":false,"LastName":"aa","Mobile":"","ModificationDateTime":"2022-05-09T13:40:31Z","Phone":"","Profile":{"Data":{"InternalID":69004,"Name":"Rep"},"URI":"/profiles/69004"},"Role":null}')
            )
    }

    const papiService = new PapiService(papiClient);

    const request: Request = {
        method: 'GET',
        body: {},
        header: {},
        query:
        {
            resource_name: 'users',
        }
    }

    it('should return a valid item - get by Key', async () => {

        const requestCopy = {...request};
        const queryCopy = {...requestCopy.query};
        queryCopy.field_id = 'Key';
        queryCopy.value = requestedKey;

        requestCopy.query = queryCopy;

        await getByUniqueField(requestCopy, papiService, requestedKey);
    })

    it('should return a valid item - get by UUID', async () => {

        const requestCopy = {...request};
        const queryCopy = {...requestCopy.query};
        queryCopy.field_id = 'UUID';
        queryCopy.value = requestedUUID;

        requestCopy.query = queryCopy;


        await getByUniqueField(requestCopy, papiService, requestedKey);
    })

    it('should return a valid item - get by InternalID', async () => {

        const requestCopy = {...request};
        const queryCopy = {...requestCopy.query};

        queryCopy.field_id = 'InternalID';
        queryCopy.value = requestedInternalID;

        requestCopy.query = queryCopy;


        await getByUniqueField(requestCopy, papiService, requestedKey);
    })

    it('should return a valid item - get by ExternalID', async () => {
        
        const requestCopy = {...request};
        const queryCopy = {...requestCopy.query};

        queryCopy.field_id = 'ExternalID';
        queryCopy.value = requestedExternalID;

        requestCopy.query = queryCopy;

        await getByUniqueField(requestCopy, papiService, requestedKey);
    })

    it('should throw an invalid unique field exception', async () => {
        
        const requestCopy = {...request};
        const queryCopy = {...requestCopy.query};

        queryCopy.field_id = 'FAULTY_FIELD';
        queryCopy.value = requestedExternalID;

        requestCopy.query = queryCopy;

        const core = new CoreService(request.query.resource_name, requestCopy, papiService);

        await expect(core.getResourceByUniqueField()).to.be.rejectedWith(`The field_id query parameter is not valid. Supported field_ids are: ${UNIQUE_FIELDS.join(", ")}`); 
    })

    it('should throw a missing field_id exception', async () => {
        
        const requestCopy = {...request};
        const queryCopy = {...requestCopy.query};

        queryCopy.value = requestedExternalID;

        requestCopy.query = queryCopy;

        const core = new CoreService(request.query.resource_name, requestCopy, papiService);

        await expect(core.getResourceByUniqueField()).to.be.rejectedWith(`Missing the required field_id and value query parameters.`); 
    })

    it('should throw a missing value parameter exception', async () => {
        
        const requestCopy = {...request};
        const queryCopy = {...requestCopy.query};

        queryCopy.field_id = 'ExternalID';

        requestCopy.query = queryCopy;

        const core = new CoreService(request.query.resource_name, requestCopy, papiService);

        await expect(core.getResourceByUniqueField()).to.be.rejectedWith(`Missing the required field_id and value query parameters.`); 
    })

    it('should throw an "invalid resource" exception', async () => {
        const papiClient = new PapiClient({
            baseURL: mockClient.BaseURL,
            token: mockClient.OAuthAccessToken,
            addonUUID: mockClient.AddonUUID,
            actionUUID: mockClient.ActionUUID,
        });

            const papiService = new PapiService(papiClient);

            const request: Request = {
                method: 'POST',
                body: {},
                header: {},
                query:
                {
                    resource_name: 'FAULTY_RESOURCE',
                    key: requestedKey
                }
            }

            expect(() => new CoreSchemaService(request.query.resource_name, request, papiService)).to.throw('The resource name is not valid. Please provide a valid resource name.');
        }
    )
});

async function getByUniqueField(request: Request, papiService: PapiService, requestedKey: string) {
    const core = new CoreService(request.query.resource_name, request, papiService);

    const item = await core.getResourceByUniqueField();

    expect(item).to.be.an('object');
    expect(item).to.have.property('Key', requestedKey);
}