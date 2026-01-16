import chai, { expect } from 'chai';

import chaiAsPromised from 'chai-as-promised';

import { Chance } from 'chance';

import bcrypt from 'bcrypt';

import { authenticateController } from '../../../src/controllers/auth';

import { Member } from '../../../src/lib/mongoose/models/member';

import { initializeDatabase } from '../../../src/lib/mongoose';

chai.use(chaiAsPromised);

const chance = new Chance();

describe('Auth Controllers', () => {
  before(async function () {
    this.mock = null;
    this.randomUsername = () => chance.word();
    this.randomPassword = () => chance.word();
    this.randomRealName = () => chance.name({ middle: true });
    await initializeDatabase();

    this.validUsername = this.randomUsername();
    this.validPassword = this.randomPassword();

    await Member.create({
      username: this.validUsername,
      password: await bcrypt.hash(this.validPassword, 10),
      realName: this.randomRealName(),
    });
  });

  after(() => {
    return Member.deleteMany({});
  });

  describe('Member Authentication', () => {
    describe('GIVEN correct username and password', () => {
      it('should return success status code', async function () {
        this.mock = {
          body: {
            username: this.validUsername,
            password: this.validPassword,
          },
          query: null,
          params: null,
          ip: null,
          method: null,
          path: null,
          headers: {
            'Content-Type': null,
            Referer: null,
            'User-Agent': null,
          },
        };

        await expect(
          authenticateController(this.mock),
        ).to.eventually.fulfilled.property('statusCode', 200);
      });
    });

    describe('GIVEN incorrect username', () => {
      it('should return error status code', async function () {
        this.mock = {
          body: {
            username: this.randomUsername(),
            password: this.validPassword,
          },
          query: null,
          params: null,
          ip: null,
          method: null,
          path: null,
          headers: {
            'Content-Type': null,
            Referer: null,
            'User-Agent': null,
          },
        };

        await expect(
          authenticateController(this.mock),
        ).to.eventually.fulfilled.property('statusCode', 400);
      });
    });

    describe('GIVEN incorrect password', () => {
      it('should return error status code', async function () {
        this.mock = {
          body: {
            username: this.validUsername,
            password: this.randomPassword(),
          },
          query: null,
          params: null,
          ip: null,
          method: null,
          path: null,
          headers: {
            'Content-Type': null,
            Referer: null,
            'User-Agent': null,
          },
        };

        await expect(
          authenticateController(this.mock),
        ).to.eventually.fulfilled.property('statusCode', 400);
      });
    });

    describe('GIVEN no username', () => {
      it('should return error status code', async function () {
        this.mock = {
          body: {
            username: '',
            password: this.validPassword,
          },
          query: null,
          params: null,
          ip: null,
          method: null,
          path: null,
          headers: {
            'Content-Type': null,
            Referer: null,
            'User-Agent': null,
          },
        };

        await expect(
          authenticateController(this.mock),
        ).to.eventually.fulfilled.property('statusCode', 400);
      });
    });

    describe('GIVEN no password', () => {
      it('should return error status code', async function () {
        this.mock = {
          body: {
            username: this.validUsername,
            password: '',
          },
          query: null,
          params: null,
          ip: null,
          method: null,
          path: null,
          headers: {
            'Content-Type': null,
            Referer: null,
            'User-Agent': null,
          },
        };

        await expect(
          authenticateController(this.mock),
        ).to.eventually.fulfilled.property('statusCode', 400);
      });
    });
  });
});