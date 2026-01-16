import chai, { expect } from 'chai';

import chaiAsPromised from 'chai-as-promised';

import bcrypt from 'bcrypt';

import mongoose from 'mongoose';

import { Chance } from 'chance';

import { Member } from '../../../src/lib/mongoose/models/member';
import { Promo } from '../../../src/lib/mongoose/models/promo';
import { PromoEnrollment } from '../../../src/lib/mongoose/models/promo-enrollment';

import { promoEnrollmentsStore } from '../../../src/data-access/mongoose/promo-enrollments';

import { initializeDatabase } from '../../../src/lib/mongoose';

import {
  PromoTemplate,
  PromoStatus,
  RequiredMemberFields,
  PromoEnrollmentStatus,
} from '../../../src/types';

const { insertPromoEnrollment } = promoEnrollmentsStore;

chai.use(chaiAsPromised);

const chance = new Chance();

describe('Promo Enrollment Data Access', function () {
  before(async function () {
    await initializeDatabase();
    this.mockedId = mongoose.Types.ObjectId().toString();
    this.mock = null;

    this.randomString = () => chance.word();

    const member = await Member.create({
      username: this.randomString(),
      password: await bcrypt.hash(this.randomString(), 10),
      realName: this.randomString(),
      email: this.randomString(),
      bankAccount: this.randomString(),
      balance: 26,
    });

    this.member = member;
  });

  after(() => {
    return Member.deleteMany({});
  });

  describe('Enroll Member to a Promo', () => {
    before(async function () {
      const depositMock = await Promo.create({
        name: this.randomString(),
        template: PromoTemplate.Deposit,
        title: this.randomString(),
        description: this.randomString(),
        minimumBalance: 25,
        status: PromoStatus.Active,
      });

      const signUpMock = await Promo.create({
        name: this.randomString(),
        template: PromoTemplate.SignUp,
        title: this.randomString(),
        description: this.randomString(),
        requiredMemberFields: [
          RequiredMemberFields.BankAccount,
          RequiredMemberFields.Email,
          RequiredMemberFields.Realname,
        ],
        status: PromoStatus.Active,
      });
      this.depositMockId = depositMock._id;
      this.signUpMockId = signUpMock._id;
    });

    after(() => {
      return Promo.deleteMany({});
    });

    afterEach(() => {
      return PromoEnrollment.deleteMany({});
    });

    beforeEach(() => {
      return PromoEnrollment.deleteMany({});
    });

    describe('Given promo and member with no status', () => {
      it('should be fulfilled and have status of PENDING', async function () {
        this.mock = {
          promo: this.depositMockId,
          member: this.member._id,
        };

        await expect(
          insertPromoEnrollment(this.mock),
        ).to.eventually.fulfilled.property(
          'status',
          PromoEnrollmentStatus.Pending,
        );
      });
    });

    describe('Given promo and member status', () => {
      it('should be fulfilled and have status of Approved', async function () {
        this.mock = {
          promo: this.depositMockId,
          member: this.member._id,
          status: PromoEnrollmentStatus.Approved,
        };

        await expect(
          insertPromoEnrollment(this.mock),
        ).to.eventually.fulfilled.property(
          'status',
          PromoEnrollmentStatus.Approved,
        );
      });
    });

    describe('Given no promo', () => {
      it('should be rejected', async function () {
        this.mock = {
          member: this.member._id,
        };

        await expect(insertPromoEnrollment(this.mock)).to.eventually.rejected;
      });
    });

    describe('Given no member', () => {
      it('should be rejected', async function () {
        this.mock = {
          promo: this.depositMockId,
        };

        await expect(insertPromoEnrollment(this.mock)).to.eventually.rejected;
      });
    });

    describe('Given invalid status', () => {
      it('should be rejected', async function () {
        this.mock = {
          promo: this.depositMockId,
          member: this.member._id,
          status: this.randomString(),
        };

        await expect(insertPromoEnrollment(this.mock)).to.eventually.rejected;
      });
    });
  });
});