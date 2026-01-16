import request from 'supertest';

// Include app for super agent, not server
import app from '../../../../src/app';

// Include database connection
import dbSetup from '../../../../src/configs/setup/database';

// Import model for pre-operation before asserting API methods
import Internships from '../../../../src/models/sequelize/Internships';
import Students from '../../../../src/models/sequelize/Students';
import Campaigns from '../../../../src/models/sequelize/Campaigns';
import Mentors from '../../../../src/models/sequelize/Mentors';

import { INTERNSHIP_MODE, INTERNSHIP_RESULT } from '../../../../src/internship';

import {
    defaultCampaigns,
    defaultInternships,
    defaultStudents,
    defaultMentors,
} from '../../../../__mocks__/mockData';

const baseURL = `/api/${process.env.INTERNSHIP_ENIB_API_VERSION}`;

jest.setTimeout(30000);

beforeAll((done) => {
    dbSetup.then(() => done()).catch((e) => done(e));
});

describe('POST /internships/:id/fsm', () => {
    beforeEach(async () => {
        // Remove all
        await Internships.destroy({ where: {} });
    });

    it('NoInternships_204', async () => {
        const QUERY = { state: INTERNSHIP_MODE.PUBLISHED };
        const RESPONSE = await request(app)
            .post(`${baseURL}/internships/10/fsm`)
            .send(QUERY);
        expect(RESPONSE.status).toBe(204);
    });

    it('BadRequest_400', async () => {
        const RESPONSE = await request(app).post(`${baseURL}/internships/{falseEncoding}/fsm`);
        expect(RESPONSE.status).toBe(400);
    });

    it('WrongBody_MissingResult_400', async () => {
        const QUERY = { state: INTERNSHIP_MODE.ARCHIVED };
        const RESPONSE = await request(app)
            .post(`${baseURL}/internships/10/fsm`)
            .send(QUERY);
        expect(RESPONSE.status).toBe(400);
    });

    it('WrongBody_MissingCampaignId_400', async () => {
        const QUERY = { state: INTERNSHIP_MODE.AVAILABLE_CAMPAIGN };
        const RESPONSE = await request(app)
            .post(`${baseURL}/internships/10/fsm`)
            .send(QUERY);
        expect(RESPONSE.status).toBe(400);
    });

    it('WrongBody_MissingMentorId_400', async () => {
        const QUERY = { state: INTERNSHIP_MODE.ATTRIBUTED_MENTOR };
        const RESPONSE = await request(app)
            .post(`${baseURL}/internships/10/fsm`)
            .send(QUERY);
        expect(RESPONSE.status).toBe(400);
    });

    it('WrongBody_MissingStudentId_400', async () => {
        const QUERY = { state: INTERNSHIP_MODE.ATTRIBUTED_STUDENT };
        const RESPONSE = await request(app)
            .post(`${baseURL}/internships/10/fsm`)
            .send(QUERY);
        expect(RESPONSE.status).toBe(400);
    });

    it('ChangeState_ARCHIVED_200', async () => {
        const QUERY = { state: INTERNSHIP_MODE.ARCHIVED, result: INTERNSHIP_RESULT.CANCELED };
        const VALID_INTERNSHIP = defaultInternships();

        const CREATED = await Internships.create(VALID_INTERNSHIP);
        const RESPONSE = await request(app)
            .post(`${baseURL}/internships/${CREATED.id}/fsm`)
            .send(QUERY);
        expect(RESPONSE.status).toBe(200);
        expect(RESPONSE.body).toMatchSnapshot({
            createdAt: expect.any(String),
            updatedAt: expect.any(String),
            id: expect.any(Number),
        });
    });

    it('ChangeState_PUBLISHED_200', async () => {
        const QUERY = { state: INTERNSHIP_MODE.PUBLISHED };
        const VALID_INTERNSHIP = defaultInternships();
        VALID_INTERNSHIP.state = INTERNSHIP_MODE.WAITING;

        const CREATED = await Internships.create(VALID_INTERNSHIP);
        const RESPONSE = await request(app)
            .post(`${baseURL}/internships/${CREATED.id}/fsm`)
            .send(QUERY);
        expect(RESPONSE.status).toBe(200);
        expect(RESPONSE.body).toMatchSnapshot({
            createdAt: expect.any(String),
            updatedAt: expect.any(String),
            publishAt: expect.any(Number),
            id: expect.any(Number),
        });
    });

    it('ChangeState_ATTRIBUTED_STUDENT_200', async () => {
        const STUDENT = await Students.create(defaultStudents());

        const QUERY = { state: INTERNSHIP_MODE.ATTRIBUTED_STUDENT, studentId: STUDENT.id };
        const VALID_INTERNSHIP = defaultInternships();
        VALID_INTERNSHIP.state = INTERNSHIP_MODE.PUBLISHED;

        const CREATED = await Internships.create(VALID_INTERNSHIP);
        const RESPONSE = await request(app)
            .post(`${baseURL}/internships/${CREATED.id}/fsm`)
            .send(QUERY);
        expect(RESPONSE.status).toBe(200);
        expect(RESPONSE.body).toMatchSnapshot({
            createdAt: expect.any(String),
            updatedAt: expect.any(String),
            id: expect.any(Number),
        });
    });

    it('ChangeState_CAMPAIGN_AVAILABLE_200', async () => {
        const CAMPAIGN = await Campaigns.create(defaultCampaigns());

        const QUERY = { state: INTERNSHIP_MODE.AVAILABLE_CAMPAIGN, campaignId: CAMPAIGN.id };
        const VALID_INTERNSHIP = defaultInternships();
        VALID_INTERNSHIP.state = INTERNSHIP_MODE.ATTRIBUTED_STUDENT;

        const CREATED = await Internships.create(VALID_INTERNSHIP);
        const RESPONSE = await request(app)
            .post(`${baseURL}/internships/${CREATED.id}/fsm`)
            .send(QUERY);
        expect(RESPONSE.status).toBe(200);
        expect(RESPONSE.body).toMatchSnapshot({
            createdAt: expect.any(String),
            updatedAt: expect.any(String),
            id: expect.any(Number),
        });
    });

    it('ChangeState_ATTRIBUTED_MENTOR_200', async () => {
        const MENTOR = await Mentors.create(defaultMentors());

        const QUERY = { state: INTERNSHIP_MODE.ATTRIBUTED_MENTOR, mentorId: MENTOR.id };
        const VALID_INTERNSHIP = defaultInternships();
        VALID_INTERNSHIP.state = INTERNSHIP_MODE.AVAILABLE_CAMPAIGN;
        VALID_INTERNSHIP.availableCampaign = defaultCampaigns();

        const CREATED = await Internships.create(VALID_INTERNSHIP, {
            include: [{ association: Internships.associations.availableCampaign }],
        });
        const RESPONSE = await request(app)
            .post(`${baseURL}/internships/${CREATED.id}/fsm`)
            .send(QUERY);
        expect(RESPONSE.status).toBe(200);
        expect(RESPONSE.body).toMatchSnapshot({
            createdAt: expect.any(String),
            updatedAt: expect.any(String),
            id: expect.any(Number),
        });
    });

    it('ChangeState_RUNNING_200', async () => {
        const QUERY = { state: INTERNSHIP_MODE.RUNNING, endAt: 21356456125645 };
        const VALID_INTERNSHIP = defaultInternships();
        VALID_INTERNSHIP.state = INTERNSHIP_MODE.ATTRIBUTED_MENTOR;

        const CREATED = await Internships.create(VALID_INTERNSHIP);
        const RESPONSE = await request(app)
            .post(`${baseURL}/internships/${CREATED.id}/fsm`)
            .send(QUERY);
        expect(RESPONSE.status).toBe(200);
        expect(RESPONSE.body).toMatchSnapshot({
            createdAt: expect.any(String),
            updatedAt: expect.any(String),
            startAt: expect.any(Number),
            id: expect.any(Number),
        });
    });

    it('ChangeState_VALIDATION_200', async () => {
        const QUERY = { state: INTERNSHIP_MODE.VALIDATION };
        const VALID_INTERNSHIP = defaultInternships();
        VALID_INTERNSHIP.state = INTERNSHIP_MODE.RUNNING;

        const CREATED = await Internships.create(VALID_INTERNSHIP);
        const RESPONSE = await request(app)
            .post(`${baseURL}/internships/${CREATED.id}/fsm`)
            .send(QUERY);
        expect(RESPONSE.status).toBe(200);
        expect(RESPONSE.body).toMatchSnapshot({
            createdAt: expect.any(String),
            updatedAt: expect.any(String),
            id: expect.any(Number),
        });
    });

    it('ChangeState_WAITING_200', async () => {
        const QUERY = { state: INTERNSHIP_MODE.WAITING };
        const VALID_INTERNSHIP = defaultInternships();
        VALID_INTERNSHIP.state = INTERNSHIP_MODE.PUBLISHED;

        const CREATED = await Internships.create(VALID_INTERNSHIP);
        const RESPONSE = await request(app)
            .post(`${baseURL}/internships/${CREATED.id}/fsm`)
            .send(QUERY);
        expect(RESPONSE.status).toBe(200);
        expect(RESPONSE.body).toMatchSnapshot({
            createdAt: expect.any(String),
            updatedAt: expect.any(String),
            id: expect.any(Number),
        });
    });
});