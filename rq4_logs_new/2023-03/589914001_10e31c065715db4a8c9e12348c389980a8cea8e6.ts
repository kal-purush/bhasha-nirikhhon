import request from 'supertest';
import app from '@lentovaraukset/backend/src/app';
import { Airfield } from '@lentovaraukset/backend/src/models';
import { connectToDatabase, sequelize } from '../src/util/db';

const api = request(app);

const airfields = [
  { name: 'EFHK', maxConcurrentFlights: 2, eventGranularityMinutes: 20 },
  { name: 'EFTU', maxConcurrentFlights: 1, eventGranularityMinutes: 20 },
  { name: 'EFOU', maxConcurrentFlights: 1, eventGranularityMinutes: 20 },
];

beforeAll(async () => {
  await connectToDatabase();
});

beforeEach(async () => {
  // wipe db before each test
  await sequelize.truncate({ cascade: true });
  await Airfield.bulkCreate(airfields);
});

afterAll(async () => {
  // otherwise Jest needs --forceExit
  await sequelize.close();
});

describe('Calls to api', () => {
  test('can create an airfield', async () => {
    const res = await api.post('/api/airfields/')
      .set('Content-type', 'application/json')
      .send({ name: 'EFKK', maxConcurrentFlights: 1, eventGranularityMinutes: 10 });

    const createdAirfield: Airfield | null = await Airfield.findOne(
      { where: { name: 'EFKK' } },
    );
    const numberOfAirfields: Number = await Airfield.count();

    expect(res.status).toEqual(200);
    expect(numberOfAirfields).toEqual(airfields.length + 1);
    expect(createdAirfield).not.toEqual(null);
    expect(createdAirfield?.dataValues.maxConcurrentFlights).toEqual(1);
    expect(createdAirfield?.dataValues.eventGranularityMinutes).toEqual(10);
  });

  test('dont create an airfield if all fields not provided', async () => {
    const res = await api.post('/api/airfields/')
      .set('Content-type', 'application/json')
      .send({ name: 'EFKK' });

    const createdAirfield: Airfield | null = await Airfield.findOne(
      { where: { name: 'EFKK' } },
    );
    const numberOfAirfields: Number = await Airfield.count();

    expect(res.body.error.message).toContain('Expected number');
    expect(numberOfAirfields).toEqual(airfields.length);
    expect(createdAirfield).toEqual(null);
  });

  test('dont create an airfield if name already exists', async () => {
    const res = await api.post('/api/airfields/')
      .set('Content-type', 'application/json')
      .send({ name: 'EFHK', maxConcurrentFlights: 2, eventGranularityMinutes: 20 });

    const numberOfAirfields: Number = await Airfield.count();

    expect(res.body.error.message).toContain('already exists');
    expect(numberOfAirfields).toEqual(airfields.length);
  });
});