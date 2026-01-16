const server = require('../api/server.js');
const request = require('supertest');
const db = require('../data/db-config.js');

const authRouter = ('./auth-router.js');

const jwt = require('jsonwebtoken');
const { jwtSecret } = require('../config/secrets');

const authenticate = require("../middleware/auth-jest.js");

describe('authRouter', function() {

  it('runs the tests', async function() {
    await expect(true).toBe(true);
  });

  describe ("test environment", function() {
    it("should use the testing environment", async function() {
      await expect(process.env.DB_ENV).toBe("testing");
    });
  });

  describe('confirms the server is running (get /)', function() {
    it('should return 200 OK', async function() {
      const res = await request(server).get('/api/auth')
      expect(res.status).toBe(200);
      expect(res.type).toMatch(/json/i);
    });
  });

  // LOGINS
  describe('Business logins are working', function() {
    it('should return 200 OK on successful login', async function() {
      const res = await request(server).post('/api/auth/login/business')
        .send({
          email: "manager@costco.com",
          password: "bulk"
        });

        expect(res.status).toBe(200);
        expect(res.type).toMatch(/json/i);
    }); // it

    it('successful login should receive a token', async function() {
      const res = await request(server).post('/api/auth/login/business')
        .send({
          email: "manager@costco.com",
          password: "bulk"
        });
        expect(res.body.token).toBeDefined();
    }); // it

    it('should fail a password mismatch', async function() {
      const res = await request(server).post('/api/auth/login/business')
        .send({
          email: "manager@costco.com",
          password: "not my real password"
        });

        expect(res.status).toBe(401);
        expect(res.type).toMatch(/json/i);
    })

    it('failed login should not receive a token', async function() {
      const res = await request(server).post('/api/auth/login/business')
        .send({
          email: "manager@costco.com",
          password: "fake password"
        });
        expect(res.body.token).not.toBeDefined();
    }); // it
  }); // describe business logins

  describe('Volunteer logins are working', function() {
    it('should return 200 OK on successful login', async function() {
      const res = await request(server).post('/api/auth/login/volunteer')
        .send({
          email: "bike@couriers.com",
          password: "bike"
        });
        expect(res.status).toBe(200);
        expect(res.type).toMatch(/json/i);
    }); // it

    it('should receive a token on successful login ', async function() {
      const res = await request(server).post('/api/auth/login/volunteer')
        .send({
          email: "bike@couriers.com",
          password: "bike"
        });
        expect(res.body.token).toBeDefined();
    }); // it

    it('should fail a password mismatch', async function() {
      const res = await request(server).post('/api/auth/login/volunteer')
        .send({
          email: "bike@couriers.com",
          password: "not my real password"
        });

        expect(res.status).toBe(401);
        expect(res.type).toMatch(/json/i);
    })

    it('should not receive a token on failed login', async function() {
      const res = await request(server).post('/api/auth/login/volunteer')
        .send({
          email: "bike@couriers.com",
          password: "fake password"
        });
        expect(res.body.token).not.toBeDefined();
    }); // it
  }); // describe volunteer logins


  // SIGNUPS (REGISTRATION) 

  describe('Business Registrations are working', function() {
    it('valid info should return 201 (created)', async function() {
      const res = await request(server).post('/api/auth/register/business')
        .send({
          email: "george.washington@whitehouse.gov",
          password: "george",
          name: "George W.",
          address: "1600 Pennsylvania Ave.",
          description: "The big white house with the pillars.",
          phone: "(202) 0000-1111"
        });

      expect(res.status).toBe(201);
      expect(res.type).toMatch(/json/i);
    });

    // FIXME: potential problem if *email* field is missing
    it('should fail if required fields are missing', async function() {
      const res = await request(server).post('/api/auth/register/business')
        .send({
          email: "george@washington.com",
          password: "george",
          // name: "George W.",
          address: "1600 Pennsylvania Ave.",
          description: "The big white house with the pillars.",
          phone: "(202) 0000-1111"
        });

      expect(res.status).toBe(400);
      expect(res.type).toMatch(/json/i);
    });

    it('should fail if an existing user tries to register.', async function() {
      const res = await request(server).post('/api/auth/register/business')
        .send({
          email: "manager@costco.com",
          password: "bulk",
          name: "Costco Wholesale",
          address: "1600 Pennsylvania Ave.",
          description: "The big white house with the pillars.",
          phone: "(202) 0000-1111"
        });

      expect(res.status).toBe(500);
      expect(res.type).toMatch(/json/i);
    });

  }); // describe business registrations

  describe('Volunteer Registrations are working', function() {
    it('valid info should return 201 (created)', async function() {
      const res = await request(server).post('/api/auth/register/volunteer')
          .send({
            email: "salvation@army.org",
            password: "army",
            name: "Salvation Army",
            phone: "(202) 0000-1111"
          });

      expect(res.status).toBe(201);
      expect(res.type).toMatch(/json/i);
    });

    // FIXME: potential problem if *email* field is missing
    it('should fail if required fields are missing', async function() {
      const res = await request(server).post('/api/auth/register/volunteer')
        .send({
          email: "tim@smith.com",
          password: "timmy",
          // name: "Tim",
          phone: "123.456.7890"
        });
      });
    //   expect(res.status).toBe(400);
    //   expect(res.type).toMatch(/json/i);
    // });

    it('should fail if an existing user tries to register.', async function() {
      const res = await request(server).post('/api/auth/register/volunteer')
        .send({
          email: "bike@couriers.com",
          password: "bike",
          name: "Bike Person",
          phone: "(202) 0000-1111"
        });
        expect(res.status).toBe(500);
        expect(res.type).toMatch(/json/i);
    });
  }); // describe volunteer registrations
}); // describe authRouter