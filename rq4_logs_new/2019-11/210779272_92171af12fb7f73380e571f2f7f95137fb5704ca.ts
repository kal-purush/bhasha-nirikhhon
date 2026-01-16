import request from 'supertest';

// Include app for super agent, not server
import app from '../../../../src/app';

// Include database connection
import dbSetup from '../../../../src/configs/setup/database';

import Campaigns from '../../../../src/models/Campaigns';

import cache from '../../../../src/statistics/singleton';

import { defaultCampaigns } from '../../../../__mocks__/mockData';

const baseURL = `/api/${process.env.INTERNSHIP_ENIB_API_VERSION}`;

jest.setTimeout(30000);

beforeAll((done) => {
    dbSetup.then(() => done()).catch((e) => done(e));
});

describe('GET /statistics', () => {
    it('Default 200 Statistics', async () => {
        const RESPONSE = await request(app).get(`${baseURL}/statistics`);
        expect(RESPONSE.status).toBe(200);
        expect(RESPONSE.body).toMatchSnapshot();
    });
});

describe('GET /campaigns/:id/statistics', () => {
    beforeEach(async () => {
        // Remove all
        await Campaigns.destroy({ where: {} });
    });

    it('NoCampaign_204', async () => {
        const RESPONSE = await request(app).get(`${baseURL}/campaigns/10/statistics`);
        expect(RESPONSE.status).toBe(204);
    });

    it('BadRequest_400_WrongID', async () => {
        const RESPONSE = await request(app).get(`${baseURL}/campaigns/{falseEncoding}/statistics`);
        expect(RESPONSE.status).toBe(400);
    });

    it('Campaigns_200_NoInternship', async () => {
        // In this case, we check if link a existing Bussiness and an unexisting internships work
        const VALID_CAMPAIGN = defaultCampaigns();

        const CREATED = await Campaigns.create(VALID_CAMPAIGN);
        cache.newCampain(Number(CREATED.id), {});

        const RESPONSE = await request(app).get(`${baseURL}/campaigns/${CREATED.id}/statistics`);
        expect(RESPONSE.status).toBe(200);
        expect(RESPONSE.body).toMatchSnapshot();
    });
});