// tests related to the Toy document and its HTTP methods
import request from 'supertest';
import server from '../../server';

import Toy from '../../api/toys/toy.model';
import setupDBContainer from '../../test-setup';

const { setupDB } = setupDBContainer;

setupDB('toys-endpoint-testing', true);

//TODO: some of these tests should be split to different ones

// toys retrieving the toy list
describe('GET /', () => {
  // handle success case
  test('retrieve the correct number of toys from the database', async () => {
    const res = await request(server.callback()).get('/api/toys/');
    expect(res.status).toBe(200);
    console.log(server);
    console.log(server.callback());
    console.log(res);
    console.log(res.body);
    // Ensure that the query contains an array with the correct amount of items
    const toys = await Toy.find({});
    console.log(toys);
    console.log(toys.length);
    expect(toys.length).toBe(3);
  });
});

// retrieve only a specific toy
describe('GET /:toyId', () => {
  // handle success case
  test('retrieve the correct toy given an id', async () => {
    // note: 12-byte string is used to mock MongoDB ObjectId
    const res = await request(server.callback()).get('/api/toys/000000000001');
    expect(res.status).toBe(200);

    // Ensure it returns the correct toy object
    const toy = await Toy.findById('000000000001');
    expect(toy).toMatchObject({
      _id: '000000000001',
      name: 'Thor 1/6 Scale',
      manufacturer: 'Hot Toys',
      description:
        'Sophisticatedly crafted based on Chris Hemsworth’s appearance in the film with astonishing likeness, the Thor collectible figure features a newly developed head sculpt with specially applied luminous reflective effect on the eye that accentuates Thor using his thunder power, a newly developed muscular body with two pairs of interchangeable arms, intricately detailed body armor with LED light up circle plates and lightning effect accessories, a detachable red-colored cap, detail recreation of Thor’s weapon Stormbreaker, and a specially designed figure stand with movie logo.',
      image_url:
        'https://www.sideshow.com/storage/product-images/903422/thor_marvel_silo.png',
      franchise: 'Marvel',
      series: 'Avengers: Infinity War',
    });
  });

  //  handle fail case
  test('return status code of `404` and query return value of `null` if toy does not exist', async () => {
    // note: 12-byte string is used to mock MongoDB ObjectId
    const res = await request(server.callback()).get('/api/toys/000000000009');
    expect(res.status).toBe(404);

    // Ensure it returns null (meaning nothing was found using that id)
    const toy = await Toy.findById('000000000009');
    expect(toy).toBe(null);
  });
});

describe('POST /', () => {
  // handle success case
  test('return status code of `201` when sending valid data', async () => {
    const res = await request(server.callback()).post('/api/toys/').send({
      name: 'Thor 1/6 Scale',
      manufacturer: 'Hot Toys',
      description:
        'Sophisticatedly crafted based on Chris Hemsworth’s appearance in the film with astonishing likeness, the Thor collectible figure features a newly developed head sculpt with specially applied luminous reflective effect on the eye that accentuates Thor using his thunder power, a newly developed muscular body with two pairs of interchangeable arms, intricately detailed body armor with LED light up circle plates and lightning effect accessories, a detachable red-colored cap, detail recreation of Thor’s weapon Stormbreaker, and a specially designed figure stand with movie logo.',
      franchise: 'Marvel',
      series: 'Avengers: Infinity War',
    });

    expect(res.status).toBe(201);
  });

  test('response contains the correct properties when sending valid data', async () => {
    const res = await request(server.callback()).post('/api/toys/').send({
      name: 'Thor 1/6 Scale',
      manufacturer: 'Hot Toys',
      description:
        'Sophisticatedly crafted based on Chris Hemsworth’s appearance in the film with astonishing likeness, the Thor collectible figure features a newly developed head sculpt with specially applied luminous reflective effect on the eye that accentuates Thor using his thunder power, a newly developed muscular body with two pairs of interchangeable arms, intricately detailed body armor with LED light up circle plates and lightning effect accessories, a detachable red-colored cap, detail recreation of Thor’s weapon Stormbreaker, and a specially designed figure stand with movie logo.',
      franchise: 'Marvel',
      series: 'Avengers: Infinity War',
    });

    // Ensure that the response contains the correct properties
    expect(res.body.name).toBeTruthy();
    expect(res.body.manufacturer).toBeTruthy();
    expect(res.body.franchise).toBeTruthy();
    expect(res.body.series).toBeTruthy();
    expect(res.body.description).toBeTruthy();
  });

  test('Add toy document to the database when sending valid data', async () => {
    const res = await request(server.callback()).post('/api/toys/').send({
      name: 'Thor 1/6 Scale',
      manufacturer: 'Hot Toys',
      description:
        'Sophisticatedly crafted based on Chris Hemsworth’s appearance in the film with astonishing likeness, the Thor collectible figure features a newly developed head sculpt with specially applied luminous reflective effect on the eye that accentuates Thor using his thunder power, a newly developed muscular body with two pairs of interchangeable arms, intricately detailed body armor with LED light up circle plates and lightning effect accessories, a detachable red-colored cap, detail recreation of Thor’s weapon Stormbreaker, and a specially designed figure stand with movie logo.',
      franchise: 'Marvel',
      series: 'Avengers: Infinity War',
    });

    // check if the toy exists, with correct properties and values, in the database
    const toy = await Toy.findById(res.body.id);
    expect(toy.name).toBeTruthy();
    expect(toy.manufacturer).toBeTruthy();
    expect(toy.franchise).toBeTruthy();
    expect(toy.series).toBeTruthy();
    expect(toy.description).toBeTruthy();
  });

  // handle fail case
  test('missing toy name should return status code `400` and error message', async () => {
    const res = await request(server.callback()).post('/api/toys/').send({
      description: 'toy without a name',
    });
    expect(res.status).toBe(400);

    // .toContain is used because the error message's wording could be changed
    expect(res.body.status).toContain('fail');
    expect(res.body.message).toContain('toy should have a name');
  });
});

describe('PATCH /:toyId', () => {
  // handle success case
  test('return status code `200` when valid data is sent', async () => {
    const res = await request(server.callback())
      .patch('/api/toys/000000000001')
      .send({
        name: 'Captain America',
        manufacturer: 'Hot Toys',
        description:
          'Authentic and detailed likeness of Chris Evans as Captain America in Avengers: Endgame',
        franchise: 'Marvel',
        series: 'Avengers: Endgame',
      });

    expect(res.status).toBe(200);
  });

  test('response body returns updated values when valid data is sent', async () => {
    const res = await request(server.callback())
      .patch('/api/toys/000000000001')
      .send({
        name: 'Captain America',
        manufacturer: 'Hot Toys',
        description:
          'Authentic and detailed likeness of Chris Evans as Captain America in Avengers: Endgame',
        franchise: 'Marvel',
        series: 'Avengers: Endgame',
      });

    // Ensure that the response contains the updated values
    expect(res.body.name).toBe('Captain America');
    expect(res.body.manufacturer).toBe('Hot Toys');
    expect(res.body.franchise).toBe('Marvel');
    expect(res.body.series).toBe('Avengers: Endgame');
    expect(res.body.description).toBe(
      'Authentic and detailed likeness of Chris Evans as Captain America in Avengers: Endgame',
    );
  });

  test('updates toy document in the database when valid data is sent', async () => {
    const res = await request(server.callback())
      .patch('/api/toys/000000000001')
      .send({
        name: 'Captain America',
        manufacturer: 'Hot Toys',
        description:
          'Authentic and detailed likeness of Chris Evans as Captain America in Avengers: Endgame',
        franchise: 'Marvel',
        series: 'Avengers: Endgame',
      });

    // check if the toy exists in the database
    const toy = await Toy.findById(res.body.id);
    expect(toy.name).toBe('Captain America');
    expect(toy.manufacturer).toBe('Hot Toys');
    expect(toy.franchise).toBe('Marvel');
    expect(toy.series).toBe('Avengers: Endgame');
    expect(toy.description).toBe(
      'Authentic and detailed likeness of Chris Evans as Captain America in Avengers: Endgame',
    );
  });

  // handle fail case
  // missing name property
  test('missing toy name should return status code `400` and error message', async () => {
    const res = await request(server.callback())
      .patch('/api/toys/:toyId')
      .send({
        description: 'toy without a name',
      });

    expect(res.status).toBe(400);
    // .toContain is used because the error message's wording could be changed
    expect(res.body.status).toContain('fail');
    expect(res.body.message).toContain('toy should have a name');
  });

  // not found because of wrong id
  test('return status code of `404` and query return value of `null` if toy does not exist', async () => {
    // note: 12-byte string is used to mock MongoDB ObjectId
    const res = await request(server.callback())
      .patch('/api/toys/000000000009')
      .send({ name: 'new toy name', description: 'new description' });

    expect(res.status).toBe(404);
    // Ensure it returns null (meaning nothing was found using that id)
    const toy = await Toy.findById('000000000009');
    expect(toy).toBe(null);
  });
});

describe('DELETE /:toyId', () => {
  // handle success case
  test('return status code of `200` when deleting existing toy', async () => {
    const res = await request(server.callback()).delete(
      '/api/toys/000000000001',
    );

    expect(res.status).toBe(200);
  });

  test('remove the toy from the database when deleting existing toy', async () => {
    const res = await request(server.callback()).delete(
      '/api/toys/000000000001',
    );

    // check if the toy is no longer in the database
    const toy = await Toy.findById(res.body.id);
    expect(toy).toBe(null);
  });

  // handle fail case
  // not found because of wrong id
  test('return status code of `404` and query return value of `null` if toy does not exist', async () => {
    // note: 12-byte string is used to mock MongoDB ObjectId
    const res = await request(server.callback()).delete(
      '/api/toys/000000000009',
    );

    expect(res.status).toBe(404);
    // Ensure it returns null (meaning nothing was found using that id)
    const toy = await Toy.findById('000000000009');
    expect(toy).toBe(null);
  });
});