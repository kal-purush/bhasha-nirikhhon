import 'mocha';
import chai, { expect } from 'chai';
import promised from 'chai-as-promised';
import PapiService from '../papi.service';
import { CoreSchemaService } from '../coreSchema.service';
import { mockClient } from './consts';
import { Request } from "@pepperi-addons/debug-server";
import { PapiClient } from '@pepperi-addons/papi-sdk';
import { CoreService } from '../core.service';

chai.use(promised);

describe('GET resource by key', async () => {
    const requestedKey = 'dd51e0a9-83f3-49b5-9074-35f13916b340';

    it('should return a valid item', async () => {
        const papiClient = new PapiClient({
            baseURL: mockClient.BaseURL,
            token: mockClient.OAuthAccessToken,
            addonUUID: mockClient.AddonUUID,
            actionUUID: mockClient.ActionUUID,
        });

        papiClient.get = async (url: string) => {
            if (url === `/users/UUID/dd51e0a9-83f3-49b5-9074-35f13916b340`)
            {
                return Promise.resolve(
                    JSON.parse('{"InternalID":11496119,"UUID":"dd51e0a9-83f3-49b5-9074-35f13916b340","ExternalID":"","CreationDateTime":"2022-05-09T13:39:18Z","Email":"testing@testing.testing","FirstName":"aaa","Hidden":false,"IsInTradeShowMode":false,"LastName":"aa","Mobile":"","ModificationDateTime":"2022-05-09T13:40:31Z","Phone":"","Profile":{"Data":{"InternalID":69004,"Name":"Rep"},"URI":"/profiles/69004"},"Role":null}')
                )
            }
        }

        const papiService = new PapiService(papiClient);

        const request: Request = {
            method: 'POST',
            body: {},
            header: {},
            query:
            {
                resource_name: 'users',
                key: requestedKey
            }
        }

        const core = new CoreService(request.query.resource_name ,request, papiService);

        const item = await core.getByKey();

        expect(item).to.be.an('object');
        expect(item).to.have.property('Key', requestedKey);
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