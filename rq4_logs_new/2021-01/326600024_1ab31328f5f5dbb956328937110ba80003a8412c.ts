import chai, { expect } from 'chai';
import chaiHttp from 'chai-http';
import bcrypt from 'bcrypt';

import mongoose from 'mongoose';
import { Chance } from 'chance';
import { jsonToGraphQLQuery, EnumType } from 'json-to-graphql-query';
import server from '../../../src/index';

import { Vendor } from '../../../src/lib/mongoose/models/vendor';
import { Member } from '../../../src/lib/mongoose/models/member';

import { VendorType } from '../../../src/types';

chai.use(chaiHttp);

const chance = new Chance();

describe('Vendor Queries', function () {
  before(async function () {
    this.mockedId = mongoose.Types.ObjectId().toString();
    this.randomName = () => chance.name({ middle: true });
    this.randomRealName = () => chance.name({ middle: true });
    this.randomUsername = () => chance.word();
    this.randomPassword = () => chance.word();
    this.mock = null;
    this.request = () => chai.request(server.callback());

    const username = this.randomUsername();
    const password = this.randomPassword();

    await Member.create({
      username,
      password: await bcrypt.hash(password, 10),
      realName: this.randomRealName(),
    });

    const account = await this.request().post(`/auth`).send({
      username,
      password,
    });

    this.token = account.body.token;
  });

  after(() => {
    return Member.deleteMany({});
  });

  describe('Vendor Creation', () => {
    afterEach(() => {
      return Vendor.deleteMany({});
    });

    beforeEach(() => {
      return Vendor.deleteMany({});
    });

    describe('Given correct inputs and SEAMLESS type', () => {
      it('should return true', async function () {
        this.mock = {
          mutation: {
            createVendor: {
              __args: {
                input: {
                  name: this.randomName(),
                  type: new EnumType(VendorType.Seamless),
                },
              },
            },
          },
        };
        const query = jsonToGraphQLQuery(this.mock);
        const main = await this.request()
          .post('/graphql')
          .set('Authorization', `Bearer ${this.token}`)
          .send({ query });
        expect(main.statusCode).to.eqls(200);
        expect(main.body.data).property('createVendor', true);
      });
    });

    describe('Given correct inputs and TRANSFER type', () => {
      it('should throw an error', async function () {
        this.mock = {
          mutation: {
            createVendor: {
              __args: {
                input: {
                  name: this.randomName(),
                  type: new EnumType(VendorType.Transfer),
                },
              },
            },
          },
        };
        const query = jsonToGraphQLQuery(this.mock);
        const main = await this.request()
          .post('/graphql')
          .set('Authorization', `Bearer ${this.token}`)
          .send({ query });
        expect(main.statusCode).to.eqls(200);
        expect(main.body.data).property('createVendor', true);
      });
    });

    describe('Given no name', () => {
      it('should throw an error', async function () {
        this.mock = {
          mutation: {
            createVendor: {
              __args: {
                input: {
                  name: '',
                  type: new EnumType(VendorType.Transfer),
                },
              },
            },
          },
        };
        const query = jsonToGraphQLQuery(this.mock);
        const main = await this.request()
          .post('/graphql')
          .set('Authorization', `Bearer ${this.token}`)
          .send({ query });
        expect(main.statusCode).to.eqls(200);
        expect(main.body.errors[0].message).eqls('Please input name');
      });
    });

    describe('Given invalid type', () => {
      it('should throw an error', async function () {
        this.mock = {
          mutation: {
            createVendor: {
              __args: {
                input: {
                  name: this.randomName(),
                  type: 'qwe',
                },
              },
            },
          },
        };
        const query = jsonToGraphQLQuery(this.mock);
        const main = await this.request()
          .post('/graphql')
          .set('Authorization', `Bearer ${this.token}`)
          .send({ query });
        expect(main.statusCode).to.eqls(400);
      });
    });

    describe('Given no type', () => {
      it('should throw an error', async function () {
        this.mock = {
          mutation: {
            createVendor: {
              __args: {
                input: {
                  name: this.randomName(),
                  type: '',
                },
              },
            },
          },
        };
        const query = jsonToGraphQLQuery(this.mock);
        const main = await this.request()
          .post('/graphql')
          .set('Authorization', `Bearer ${this.token}`)
          .send({ query });
        expect(main.statusCode).to.eqls(400);
      });
    });

    describe('Given an existing vendor name', () => {
      it('should throw an error', async function () {
        const data = await Vendor.create({
          name: this.randomName(),
          type: VendorType.Seamless,
        });

        this.mock = {
          mutation: {
            createVendor: {
              __args: {
                input: {
                  name: data.name,
                  type: new EnumType(VendorType.Seamless),
                },
              },
            },
          },
        };
        const query = jsonToGraphQLQuery(this.mock);
        const main = await this.request()
          .post('/graphql')
          .set('Authorization', `Bearer ${this.token}`)
          .send({ query });
        expect(main.statusCode).to.eqls(200);
        expect(main.body.errors[0].message).eqls('Vendor already exists');
      });
    });
  });
});