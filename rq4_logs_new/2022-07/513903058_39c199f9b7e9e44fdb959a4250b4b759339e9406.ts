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

describe('Search resources', async () => {
    const resourcesList = [
        {
            "InternalID": 11443826,
            "UUID": "cc27dfe9-e87a-4710-ab24-8f703e167213",
            "ExternalID": "User0",
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
        },
        {
            "InternalID": 11496119,
            "UUID": "dd51e0a9-83f3-49b5-9074-35f13916b340",
            "ExternalID": "User1",
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
        }
    ]

    const papiClient = new PapiClient({
        baseURL: mockClient.BaseURL,
        token: mockClient.OAuthAccessToken,
        addonUUID: mockClient.AddonUUID,
        actionUUID: mockClient.ActionUUID,
    });

    papiClient.post = async (url: string, body?: any) => {
        if (url === `/users/search` && Object.keys(body).length === 0)
        {
            return Promise.resolve(resourcesList.filter(resource => !resource.Hidden));
        }
        else if(url === `/users/search` && body?.include_deleted)
        {
            return Promise.resolve(resourcesList);
        }
        else if(url === `/users/search` && body?.UUIDList)
        {
            let resources = resourcesList.filter(resource => body.UUIDList.includes(resource.UUID));
            resources = body.include_deleted ? resources : resources.filter(resource => !resource.Hidden);
            return Promise.resolve(resources);
        }
        else if(url === `/users/search` && body?.InternalIDList)
        {
            let resources = resourcesList.filter(resource => body.InternalIDList.includes(resource.InternalID));
            resources = body.include_deleted ? resources : resources.filter(resource => !resource.Hidden);
            return Promise.resolve(resources);
        }
        else if(url === `/users/search` && body?.where)
        {
            expect(body.where).to.include("ExternalID in ('");
            return Promise.resolve(resourcesList);
        }
        else{
            console.error(url);
            return Promise.resolve(resourcesList.filter(resource => !resource.Hidden));
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
        }
    }

    it('should return non-hidden items', async () => {

        const requestCopy = { ...request };
        const core = new CoreService(request.query.resource_name ,requestCopy, papiService);

        const items = await core.search();

        expect(items).to.be.an('Array');
        expect(items.length).to.equal(1);
        expect(items[0]).to.have.property('Key');
        expect(items[0]).to.have.property('Hidden', false);
        
    });

    it('InternalIDList - should return non-hidden items', async () => {

        const requestCopy = { ...request };
        requestCopy.body.InternalIDList = [11496119, 11443826];
        const core = new CoreService(request.query.resource_name ,requestCopy, papiService);

        const items = await core.search();

        expect(items).to.be.an('Array');
        expect(items.length).to.equal(1);
        expect(items[0]).to.have.property('Key');
        expect(items[0]).to.have.property('Hidden', false);
        
    });

    it('KeyList - should return non-hidden items', async () => {

        const requestCopy = { ...request };
        const body = {KeyList: ["cc27dfe9-e87a-4710-ab24-8f703e167213", "dd51e0a9-83f3-49b5-9074-35f13916b340"]};
        requestCopy.body = body;
        const core = new CoreService(request.query.resource_name ,requestCopy, papiService);

        const items = await core.search();

        expect(items).to.be.an('Array');
        expect(items.length).to.equal(1);
        expect(items[0]).to.have.property('Key');
        expect(items[0]).to.have.property('Hidden', false);
        
    });

    it('UniqueFieldID="UUID" - should return non-hidden items', async () => {

        const requestCopy = { ...request };
        const body = {
            UniqueFieldID: "UUID",
            UniqueFieldList: ["cc27dfe9-e87a-4710-ab24-8f703e167213", "dd51e0a9-83f3-49b5-9074-35f13916b340"]
        }

        requestCopy.body = body;

        const core = new CoreService(request.query.resource_name ,requestCopy, papiService);

        const items = await core.search();

        expect(items).to.be.an('Array');
        expect(items.length).to.equal(1);
        expect(items[0]).to.have.property('Key');
        expect(items[0]).to.have.property('Hidden', false);
        
    });

    it('UniqueFieldID="Key" - should return non-hidden items', async () => {

        const requestCopy = { ...request };
        const body = {
            UniqueFieldID: "Key",
            UniqueFieldList: ["cc27dfe9-e87a-4710-ab24-8f703e167213", "dd51e0a9-83f3-49b5-9074-35f13916b340"]
        }

        requestCopy.body = body;
        const core = new CoreService(request.query.resource_name ,requestCopy, papiService);

        const items = await core.search();

        expect(items).to.be.an('Array');
        expect(items.length).to.equal(1);
        expect(items[0]).to.have.property('Key');
        expect(items[0]).to.have.property('Hidden', false);
        
    });

    it('UniqueFieldID="ExternalID" - should return non-hidden items', async () => {

        const requestCopy = { ...request };
        const body = {
            UniqueFieldID: "ExternalID",
            UniqueFieldList: ["User0", "User1"]
        }

        requestCopy.body = body;

        const core = new CoreService(request.query.resource_name ,requestCopy, papiService);

        // The test is run in the papi.post function
        const items = await core.search();
        
    });

    it('UniqueFieldID="InternalID" - should return non-hidden items', async () => {

        const requestCopy = { ...request };
        const body = {
            UniqueFieldID: "InternalID",
            UniqueFieldList: [11496119, 11443826]
        }
        requestCopy.body = body;
        const core = new CoreService(request.query.resource_name ,requestCopy, papiService);

        const items = await core.search();

        expect(items).to.be.an('Array');
        expect(items.length).to.equal(1);
        expect(items[0]).to.have.property('Key');
        expect(items[0]).to.have.property('Hidden', false);
        
    });

    it('should return hidden and non-hidden items', async () => {

        const requestCopy = {...request};
        const body = {include_deleted: true};
        requestCopy.body = body;

        const core = new CoreService(request.query.resource_name ,requestCopy, papiService);

        const items = await core.search();

        expect(items).to.be.an('Array');
        expect(items.length).to.equal(2);
        for(const item of items)
        {
            expect(item).to.have.property('Key');
        }

        const hiddenItem = items.find(item => item.Hidden);
        const nonHiddenItem = items.find(item => !item.Hidden);

        // Should have a hidden item
        expect(hiddenItem).to.exist;
        // Should have a non-hidden item
        expect(nonHiddenItem).to.exist;
        
    });

    it('should throw an "The passed UniqueFieldID is not supported" exception', async () => {
        const requestCopy = { ...request };
        const body = {
            UniqueFieldID: "NotSupported",
            UniqueFieldList: ["User0", "User1"]
        }
        requestCopy.body = body;

        const core = new CoreService(request.query.resource_name ,requestCopy, papiService);

        await expect(core.search()).to.be.rejectedWith(`The passed UniqueFieldID is not supported: '${requestCopy.body.UniqueFieldID}'. Supported UniqueFieldID values are: ${JSON.stringify(UNIQUE_FIELDS)}`);

    });

    it('should throw a "Sending both KeyList and UniqueFieldList is not supported." exception', async () => {
        const requestCopy = { ...request };
        const body = {
            UniqueFieldID: "InternalID",
            UniqueFieldList: [11496119, 11443826],
            KeyList: [11496119, 11443826]
        }
        requestCopy.body = body;

        const core = new CoreService(request.query.resource_name ,requestCopy, papiService);

        await expect(core.search()).to.be.rejectedWith(`Sending both KeyList and UniqueFieldList is not supported.`);

    });

    it('should throw a "Missing UniqueFieldId parameter." exception', async () => {
        const requestCopy = { ...request };
        const body = {
            UniqueFieldList: ["User0", "User1"]
        }
        requestCopy.body = body;

        const core = new CoreService(request.query.resource_name ,requestCopy, papiService);

        await expect(core.search()).to.be.rejectedWith(`Missing UniqueFieldID parameter.`);

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