import chai, { expect } from 'chai';

import chaiAsPromised from 'chai-as-promised';

import mongoose from 'mongoose';

import { initializeDatabase } from '../../../src/lib/mongoose';

import { Vendor } from '../../../src/lib/mongoose/models/vendor';

import { VendorType } from '../../../src/types';

chai.use(chaiAsPromised);

describe('Vendor Models', () => {
  before(async function () {
    this.mock = null
    this.mockedId = mongoose.Types.ObjectId().toString();
    await initializeDatabase()
  });

  describe('Creating a vendor', () => {
    afterEach(() => {
      return Vendor.deleteMany({});
    });

    beforeEach(() => {
      return Vendor.deleteMany({});
    });

    describe('GIVEN correct inputs and SEAMLESS type', () => {
      it('should be fulfilled', async () => {
        const main = {
          name: 'Luis Angelo Belmonte',
          type: 'SEAMLESS',
        };
        await expect(Vendor.create(main)).to.eventually.fulfilled;
      });
    });

    describe('GIVEN correct inputs and TRANSFER type', () => {
      it('should be fulfilled', async () => {
        const main = {
          name: 'Luis Angelo Belmonte',
          type: 'TRANSFER',
        };
        await expect(Vendor.create(main)).to.eventually.fulfilled;
      });
    });

    describe('GIVEN no name', () => {
      it('should be rejected', async () => {
        const main = {
          name: '',
          type: 'TRANSFER',
        };
        await expect(Vendor.create(main)).to.eventually.rejected;
      });
    });

    describe('GIVEN no type', () => {
      it('should be rejected', async () => {
        const main = {
          name: 'Luis Angelo Belmonte',
          type: '',
        };
        await expect(Vendor.create(main)).to.eventually.rejected;
      });
    });

    describe('GIVEN invalid type', () => {
      it('should be rejected', async () => {
        const main = {
          name: 'Luis Angelo Belmonte',
          type: 'qwe',
        };
        await expect(Vendor.create(main)).to.eventually.rejected;
      });
    });
  });

  describe('Updating a vendor by filters', () => {
    after(() => {
      return Vendor.deleteMany({});
    });


    before(async function () {
      await Vendor.deleteMany({});
      this.mock = await Vendor.create({
        name: 'Luis Angelo Belmonte',
        type: 'SEAMLESS',
      });
    });

    describe('GIVEN correct inputs and SEAMLESS type', () => {
      it('should be fulfilled', async function () {
        const main = {
          name: 'Luis Angelo Belmonte',
          type: VendorType.Seamless,
        };

        await expect(
          Vendor.findOneAndUpdate(
            { _id: this.mock._id },
            { ...main },
            { new: true, runValidators: true },
          ),
        ).to.eventually.fulfilled;
      });
    });

    describe('GIVEN correct inputs and TRANSFER type', () => {
      it('should be fulfilled', async function () {
        const main = {
          name: 'Luis Angelo Belmonte',
          type: VendorType.Transfer,
        };

        await expect(
          Vendor.findOneAndUpdate(
            { _id: this.mock._id },
            { ...main },
            { new: true, runValidators: true },
          ),
        ).to.eventually.fulfilled;
      });
    });

    describe('GIVEN invalid type', () => {
      it('should be rejected', async function () {
        const main = {
          name: 'Luis Angelo Belmonte',
          type: '',
        };
        await expect(
          Vendor.findOneAndUpdate(
            { _id: this.mock._id },
            { ...main },
            { new: true, runValidators: true },
          ),
        ).to.eventually.rejected;
      });
    });

    describe('GIVEN no name', () => {
      it('should be rejected', async function () {
        const main = {
          name: '',
          type: VendorType.Transfer,
        };
        await expect(
          Vendor.findOneAndUpdate(
            { _id: this.mock._id },
            { ...main },
            { new: true, runValidators: true },
          ),
        ).to.eventually.rejected;
      });
    });
  });

  describe('Vendor exists', () => {
    after(() => {
      return Vendor.deleteMany({});
    });

    before(async function () {
      await Vendor.deleteMany({});
      this.mock = await Vendor.create({
        name: 'Luis Angelo Belmonte',
        type: 'SEAMLESS',
      });
    });

    describe('GIVEN existent vendor ID', () => {
      it('should be fulfilled and return true', async function () {
        await expect(Vendor.exists({ _id: this.mock._id })).to.eventually.fulfilled
          .be.true;
      });
    });

    describe('GIVEN a non existent vendor ID', () => {
      it('should be fulfilled and return true', async function () {
        await expect(Vendor.exists({ _id: this.mockedId })).to.eventually.fulfilled
          .be.false;
      });
    });
  });

  describe('Find Vendors', () => {
    it('should be fulfilled', async () => {
      await expect(Vendor.find().lean()).to.eventually.fulfilled;
    });
  });

  describe('Find One Vendor', () => {
    it('should be fulfilled', async function () {
      await expect(Vendor.findOne({ _id: this.mockedId })).to.eventually.fulfilled;
    });
  });

  describe('Delete One Vendor', () => {
    afterEach(() => {
      return Vendor.deleteMany({});
    });

    beforeEach(() => {
      return Vendor.deleteMany({});
    });

    describe('GIVEN existent vendor ID', () => {
      it('should be fulfilled and deleted count should be 1', async function () {
        this.mock = {
          name: 'Luis Angelo Belmonte',
          type: 'SEAMLESS',
        };
        const main = await expect(Vendor.create(this.mock)).to.eventually.fulfilled;

        await expect(
          Vendor.deleteOne({ _id: main._id }),
        ).to.eventually.fulfilled.property('deletedCount', 1);
      });
    });

    describe('GIVEN non existent vendor ID', () => {
      it('should be fulfilled and deleted count should be 0', async function () {
        this.mock = {
          name: 'Luis Angelo Belmonte',
          type: 'SEAMLESS',
        };
        await expect(Vendor.create(this.mock)).to.eventually.fulfilled;

        await expect(
          Vendor.deleteOne({ _id: this.mockedId }),
        ).to.eventually.fulfilled.property('deletedCount', 0);
      });
    });
  });
});