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

describe('Batch', async () => {
    const papiBatchResult = [
        {
            InternalID:21356284,
            UUID:"00000000-0000-0000-0000-000000000000",
            ExternalID:"",
            Status:"Update",
            Message:"Row updated.",
            URI:"/users/21356284"
        },
        {
            InternalID:21356294,
            UUID:"00000000-0000-0000-0000-000000000000",
            ExternalID:"",
            Status:"Ignore",
            Message:"No changes in this row. The row is being ignored.",
            URI:"/users/21356294"
        },
        {
            Key: "00000000-0000-0000-0000-000000000000",
            Status: "Error",
            Details: "General error: Upload file error, internal code = NUC"
        }
    ];
    
    const resourcesList = [
        {
            "InternalID": 21356284,
            "UUID": "34e2eec8-ac68-460f-8672-e95c17908e9e",
            "ExternalID": "MytestingAccount16",
            "City": "Raanana",
            "Country": "IL",
            "CreationDate": "2022-07-25T13:05:18Z",
            "Debts30": 0.0000,
            "Debts60": 0.0000,
            "Debts90": 0.0000,
            "DebtsAbove90": 0.0000,
            "Discount": 0.0,
            "Email": "",
            "Hidden": false,
            "Latitude": 32.184781,
            "Longitude": 34.871326,
            "Mobile": "",
            "ModificationDateTime": "2022-07-25T14:20:28Z",
            "Name": null,
            "Note": "",
            "Phone": null,
            "Prop1": "",
            "Prop2": "",
            "Prop3": "",
            "Prop4": "",
            "Prop5": "",
            "State": null,
            "Status": 2,
            "StatusName": "Submitted",
            "Street": "",
            "Type": "Customer",
            "TypeDefinitionID": 271182,
            "ZipCode": "",
            "Catalogs": {
                "Data": [],
                "URI": "/account_catalogs?where=AccountInternalID=21356284"
            },
            "Parent": null,
            "PriceList": null,
            "SpecialPriceList": null,
            "Users": {
                "Data": [],
                "URI": "/users?InternalWhereClause=AccountInternalID=21356284&InternalType=35"
            }
        },
        {
            "InternalID": 21356294,
            "UUID": "d6ea001a-6f50-4253-9ac5-b3f0583acfa4",
            "ExternalID": "MytestingAccount18",
            "City": "",
            "Country": "IL",
            "CreationDate": "2022-07-25T14:18:21Z",
            "Debts30": 0.0000,
            "Debts60": 0.0000,
            "Debts90": 0.0000,
            "DebtsAbove90": 0.0000,
            "Discount": 0.0,
            "Email": "",
            "Hidden": false,
            "Latitude": 31.046051,
            "Longitude": 34.851612,
            "Mobile": "",
            "ModificationDateTime": "2022-07-25T14:20:29Z",
            "Name": null,
            "Note": "",
            "Phone": null,
            "Prop1": "",
            "Prop2": "",
            "Prop3": "",
            "Prop4": "",
            "Prop5": "",
            "State": null,
            "Status": 2,
            "StatusName": "Submitted",
            "Street": "",
            "Type": "Customer",
            "TypeDefinitionID": 271182,
            "ZipCode": "",
            "Catalogs": {
                "Data": [],
                "URI": "/account_catalogs?where=AccountInternalID=21356294"
            },
            "Parent": null,
            "PriceList": null,
            "SpecialPriceList": null,
            "Users": {
                "Data": [],
                "URI": "/users?InternalWhereClause=AccountInternalID=21356294&InternalType=35"
            }
        }
    ]

    const papiClient = new PapiClient({
        baseURL: mockClient.BaseURL,
        token: mockClient.OAuthAccessToken,
        addonUUID: mockClient.AddonUUID,
        actionUUID: mockClient.ActionUUID,
    });

    papiClient.post = async (url: string, body?: any) => {
        if (url === `/batch/users`)
        {
            return Promise.resolve(papiBatchResult);
        }
        else if (url === `/users/search`)
        {
            expect(body).to.have.property('InternalIDList');
            expect(body.InternalIDList).to.be.an('array');
            expect(body.InternalIDList).to.have.lengthOf(papiBatchResult.length);
            expect(body.InternalIDList).to.include(21356284);
            expect(body.InternalIDList).to.include(21356294);
            return Promise.resolve(resourcesList);
        }
        else
        {
            console.error(url);
            return Promise.resolve([]);
        }
    }



    const papiService = new PapiService(papiClient);

    const request: Request = {
        method: 'POST',
        body: {
            Objects: 
            [
                { ExternalID: 'MytestingAccount16' },
                { ExternalID: 'MytestingAccount18' },
                { dsad: 'FaultyData' }
            ]
        },
        header: {},
        query:
        {
            resource_name: 'users',
        }
    }

    it('should perform batch', async () => {

        const requestCopy = { ...request };
        const core = new CoreService(request.query.resource_name ,requestCopy, papiService);

        const items = await core.batch();

        expect(items).to.be.an('Array');
        expect(items.length).to.equal(papiBatchResult.length);
        for(const item in items)
        {
            expect(items[item]).to.have.property('Key');
            expect(items[item]).to.have.property('Status');
            if(items[item].Status === 'Error')
            {
                expect(items[item]).to.have.property('Details');
            }
        }
    });

    it('should throw a "Missing an Objects array" exception', async () => {
        const requestCopy = { ...request };
        const body = {
        }
        requestCopy.body = body;

        const core = new CoreService(request.query.resource_name ,requestCopy, papiService);

        await expect(core.batch()).to.be.rejectedWith(`Missing an Objects array`);

    });

    it('should throw a "OverwriteObject parameter is not supported." exception', async () => {
        const requestCopy = { ...request };
        const body = {
            Objects:[],
            OverwriteObject: true
        }
        requestCopy.body = body;

        const core = new CoreService(request.query.resource_name ,requestCopy, papiService);

        await expect(core.batch()).to.be.rejectedWith(`OverwriteObject parameter is not supported.`);

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