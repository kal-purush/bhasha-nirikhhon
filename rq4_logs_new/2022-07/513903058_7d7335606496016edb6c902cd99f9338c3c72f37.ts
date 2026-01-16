import 'mocha';
import chai, { expect } from 'chai';
import promised from 'chai-as-promised';
import PapiService from '../papi.service';
import { CoreSchemaService } from '../coreSchema.service';
import { mockClient } from './consts';
import { Request } from "@pepperi-addons/debug-server";
import { PapiClient } from '@pepperi-addons/papi-sdk';
import { CoreService } from '../core.service';
import { doesNotReject } from 'assert';

chai.use(promised);

describe('GET resources', async () => {
    const resourcesList = [
        {
            "InternalID": 11443826,
            "UUID": "cc27dfe9-e87a-4710-ab24-8f703e167213",
            "ExternalID": "",
            "CreationDateTime": "2020-01-05T08:29:23Z",
            "Email": "pumddnx@hjqfiy.com",
            "FirstName": "test",
            "Hidden": true,
            "IsInTradeShowMode": false,
            "LastName": "Test",
            "Mobile": "",
            "ModificationDateTime": "2022-03-22T01:07:25Z",
            "Phone": "",
            "Profile": {
                "Data": {
                    "InternalID": 69005,
                    "Name": "Admin"
                },
                "URI": "/profiles/69005"
            },
            "Role": null,
            "Key": "cc27dfe9-e87a-4710-ab24-8f703e167213"
        },
        {
            "InternalID": 11496119,
            "UUID": "dd51e0a9-83f3-49b5-9074-35f13916b340",
            "ExternalID": "",
            "CreationDateTime": "2022-05-09T13:39:18Z",
            "Email": "testing@testing.testing",
            "FirstName": "aaa",
            "Hidden": false,
            "IsInTradeShowMode": false,
            "LastName": "aa",
            "Mobile": "",
            "ModificationDateTime": "2022-05-09T13:40:31Z",
            "Phone": "",
            "Profile": {
                "Data": {
                    "InternalID": 69004,
                    "Name": "Rep"
                },
                "URI": "/profiles/69004"
            },
            "Role": null,
            "Key": "dd51e0a9-83f3-49b5-9074-35f13916b340"
        }
    ]

    const papiClient = new PapiClient({
        baseURL: mockClient.BaseURL,
        token: mockClient.OAuthAccessToken,
        addonUUID: mockClient.AddonUUID,
        actionUUID: mockClient.ActionUUID,
    });

    papiClient.get = async (url: string) => {
        if (url === `/users?`)
        {
            return Promise.resolve(resourcesList.filter(resource => !resource.Hidden));
        }
        else if(url === `/users?include_deleted=true`)
        {
            return Promise.resolve(resourcesList);
        }
        else if(url === `/users?fields=UUID`)
        {
            return Promise.resolve(resourcesList.filter(resource => !resource.Hidden).map(resource => resource.UUID));
        }
        else{
            return Promise.resolve(resourcesList.filter(resource => !resource.Hidden));
        }
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

    it('should return non-hidden items', async () => {

        const core = new CoreService(request.query.resource_name ,request, papiService);

        const items = await core.getResources();

        expect(items).to.be.an('Array');
        expect(items.length).to.equal(1);
        expect(items[0]).to.have.property('Key');
        expect(items[0]).to.have.property('Hidden', false);
        
    });

    it('should return hidden and non-hidden items', async () => {

        const requestCopy = {...request};
        requestCopy.query.include_deleted = true;
        const core = new CoreService(request.query.resource_name ,requestCopy, papiService);

        const items = await core.getResources();

        expect(items).to.be.an('Array');
        expect(items.length).to.equal(2);
        const hiddenItem = items.find(item => item.Hidden);
        const nonHiddenItem = items.find(item => !item.Hidden);

        // Should have a hidden item
        expect(hiddenItem).to.exist;
        // Should have a non-hidden item
        expect(nonHiddenItem).to.exist;
        
    });

    it("should return a non-hidden item's Key only", async () => {

        const requestCopy = {...request};
        requestCopy.query.fields = 'Key';
        const core = new CoreService(request.query.resource_name ,requestCopy, papiService);

        const items = await core.getResources();

        expect(items).to.be.an('Array');
        expect(items.length).to.equal(1);
        //The item's number of properties should be 1
        expect(Object.keys(items[0]).length).to.equal(1);
        expect(items[0]).to.have.property('Key');

        
    });

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
                }
            }

            expect(() => new CoreSchemaService(request.query.resource_name, request, papiService)).to.throw('The resource name is not valid. Please provide a valid resource name.');
        }
    )
});