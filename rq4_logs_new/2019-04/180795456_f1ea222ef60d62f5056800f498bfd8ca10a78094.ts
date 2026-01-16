import mongoose from 'mongoose';
import Headline, { IHeadline } from '../../models/headline';
require('dotenv').config();

describe('Headline model', () => {
    beforeAll(async () => {
        await mongoose.connect(process.env.TEST_DB_URI, {useNewUrlParser: true});
    });

    afterAll(async () => {
        mongoose.connection.close();
    });

    it('Should save a headline', async () => {
        const headline: IHeadline = new Headline({
            source: 'test_source',
            title: 'test_title',
            content: 'test_content',
            publishedAt: new Date('2019-01-01')
        });
        const spy = jest.spyOn(headline, 'save');
        headline.save();

        expect(spy).toHaveBeenCalled();

        expect(headline).toMatchObject({
            source: expect.any(String),
            title: expect.any(String),
            content: expect.any(String),
            publishedAt: expect.any(Date),
        });
    });
});