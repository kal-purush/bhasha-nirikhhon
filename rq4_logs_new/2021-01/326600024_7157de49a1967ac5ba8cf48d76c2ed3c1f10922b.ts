// @ts-nocheck
import chai, { expect } from 'chai';

import chaiAsPromised from 'chai-as-promised';

import mongoose from 'mongoose';

import { Vendor } from '../../../src/lib/mongoose/models/vendor';

import { vendorsStore } from '../../../src/data-access/mongoose/vendors';

import { initializeDatabase } from '../../../src/lib/mongoose';

import { VendorType } from '../../../src/types';

chai.use(chaiAsPromised);

const mockedId = mongoose.Types.ObjectId().toString();

const {
  insertOneVendor,
  vendorExistsByFilter,
  selectAllVendors,
  selectOneVendorByFilters,
  updateVendorByFilters,
  deleteOneVendor,
} = vendorsStore;

describe('Vendor Store', () => {
  before(() => initializeDatabase());

  describe('Insert one Vendor', () => {
    afterEach(() => {
      return Vendor.deleteMany({});
    });

    beforeEach(() => {
      return Vendor.deleteMany({});
    });

    describe('GIVEN correct inputs and SEAMLESS type', () => {
      it('should successfully add vendor to the database', async () => {
        const mock = {
          name: 'Luis Angelo Belmonte',
          type: VendorType.Seamless,
          dateTimeCreated: new Date(),
          dateTimeUpdated: new Date(),
        };
        await expect(insertOneVendor(mock)).to.eventually.fulfilled;
      });
    });

    describe('GIVEN correct inputs and TRANSFER type', () => {
      it('should successfully add vendor to the database', async () => {
        const mock = {
          name: 'Luis Angelo Belmonte',
          type: VendorType.Transfer,
          dateTimeCreated: new Date(),
          dateTimeUpdated: new Date(),
        };
        await expect(insertOneVendor(mock)).to.eventually.fulfilled;
      });
    });

    describe('GIVEN no name', () => {
      it('should throw an error', async () => {
        const mock = {
          name: '',
          type: VendorType.Seamless,
          dateTimeCreated: new Date(),
          dateTimeUpdated: new Date(),
        };
        await expect(insertOneVendor(mock)).to.eventually.rejected;
      });
    });

    describe('GIVEN no type', () => {
      it('should throw an error', async () => {
        const mock = {
          name: 'Luis Angelo Belmonte',
          type: '',
          dateTimeCreated: new Date(),
          dateTimeUpdated: new Date(),
        };
        await expect(insertOneVendor(mock)).to.eventually.rejected;
      });
    });

    describe('GIVEN invalid type', () => {
      it('should throw an error', async () => {
        const mock = {
          name: 'Luis Angelo Belmonte',
          type: 'qwe',
          dateTimeCreated: new Date(),
          dateTimeUpdated: new Date(),
        };
        await expect(insertOneVendor(mock)).to.eventually.rejected;
      });
    });
  });

  describe('Vendor exists? By filters', () => {
    afterEach(() => {
      return Vendor.deleteMany({});
    });

    beforeEach(() => {
      return Vendor.deleteMany({});
    });

    describe('GIVEN existent filters', () => {
      it('should return true', async () => {
        const mock = {
          name: 'Luis Angelo Belmonte',
          type: VendorType.Transfer,
          dateTimeCreated: new Date(),
          dateTimeUpdated: new Date(),
        };
        const main = await insertOneVendor(mock);
        await expect(vendorExistsByFilter({ _id: main._id })).to.eventually
          .fulfilled.be.true;
      });
    });

    describe('GIVEN non existent filters', () => {
      it('should return false', async () => {
        const mock = {
          name: 'Luis Angelo Belmonte',
          type: VendorType.Transfer,
          dateTimeCreated: new Date(),
          dateTimeUpdated: new Date(),
        };
        await insertOneVendor(mock);
        await expect(vendorExistsByFilter({ _id: mockedId })).to.eventually
          .fulfilled.be.false;
      });
    });

    describe('GIVEN invalid filters', () => {
      it('should throw an error', async () => {
        const mock = {
          name: 'Luis Angelo Belmonte',
          type: VendorType.Transfer,
          dateTimeCreated: new Date(),
          dateTimeUpdated: new Date(),
        };
        await insertOneVendor(mock);
        await expect(vendorExistsByFilter({ _id: 'qwe' })).to.eventually
          .rejected;
      });
    });
  });

  describe('Select All Vendors', () => {
    afterEach(() => {
      return Vendor.deleteMany({});
    });

    beforeEach(() => {
      return Vendor.deleteMany({});
    });

    it('should return a list of vendors', async () => {
      const mock = {
        name: 'Luis Angelo Belmonte',
        type: VendorType.Transfer,
        dateTimeCreated: new Date(),
        dateTimeUpdated: new Date(),
      };
      await insertOneVendor(mock);

      await expect(selectAllVendors()).eventually.fulfilled.and.have.length(1);
    });
  });

  describe('Select One Vendor By Filters', () => {
    afterEach(() => {
      return Vendor.deleteMany({});
    });

    beforeEach(() => {
      return Vendor.deleteMany({});
    });

    describe('GIVEN existent filters', () => {
      it('should return the vendor', async () => {
        const mock = {
          name: 'Luis Angelo Belmonte',
          type: VendorType.Transfer,
          dateTimeCreated: new Date(),
          dateTimeUpdated: new Date(),
        };
        const main = await insertOneVendor(mock);
        await expect(selectOneVendorByFilters({ _id: main._id }))
          .to.eventually.fulfilled.and.property('_id')
          .eqls(main._id);
      });
    });

    describe('GIVEN non existent filters', () => {
      it('should return the vendor', async () => {
        const mock = {
          name: 'Luis Angelo Belmonte',
          type: VendorType.Transfer,
          dateTimeCreated: new Date(),
          dateTimeUpdated: new Date(),
        };
        await insertOneVendor(mock);
        await expect(selectOneVendorByFilters({ _id: mockedId })).to.eventually
          .fulfilled.and.null;
      });
    });

    describe('GIVEN invalid filters', () => {
      it('should return the vendor', async () => {
        const mock = {
          name: 'Luis Angelo Belmonte',
          type: VendorType.Transfer,
          dateTimeCreated: new Date(),
          dateTimeUpdated: new Date(),
        };
        await insertOneVendor(mock);
        await expect(selectOneVendorByFilters({ _id: 'qwe' })).to.eventually
          .rejected;
      });
    });
  });

  describe('Updating a vendor', () => {
    describe('GIVEN correct inputs and SEAMLESS type', () => {
      it('should update the vendor to SEAMLESS', async () => {
        const mock = {
          name: 'Luis Angelo Belmonte',
          type: VendorType.Transfer,
          dateTimeCreated: new Date(),
          dateTimeUpdated: new Date(),
        };
        const main = await insertOneVendor(mock);

        await expect(
          updateVendorByFilters(
            { _id: main._id },
            {
              name: 'Luis Angelo Belmonte',
              type: VendorType.Seamless,
              dateTimeCreated: new Date(),
              dateTimeUpdated: new Date(),
            },
          ),
        )
          .to.eventually.fulfilled.property('type')
          .eqls('SEAMLESS');
      });
    });

    describe('GIVEN correct inputs and TRANSFER type', () => {
      it('should update the vendor to TRANSFER', async () => {
        const mock = {
          name: 'Luis Angelo Belmonte',
          type: VendorType.Seamless,
          dateTimeCreated: new Date(),
          dateTimeUpdated: new Date(),
        };
        const main = await insertOneVendor(mock);

        await expect(
          updateVendorByFilters(
            { _id: main._id },
            {
              name: 'Luis Angelo Belmonte',
              type: VendorType.Transfer,
              dateTimeCreated: new Date(),
              dateTimeUpdated: new Date(),
            },
          ),
        )
          .to.eventually.fulfilled.property('type')
          .eqls('TRANSFER');
      });
    });

    describe('GIVEN invalid type', () => {
      it('should throw an error', async () => {
        const mock = {
          name: 'Luis Angelo Belmonte',
          type: VendorType.Seamless,
          dateTimeCreated: new Date(),
          dateTimeUpdated: new Date(),
        };
        const main = await insertOneVendor(mock);

        await expect(
          updateVendorByFilters(
            { _id: main._id },
            {
              name: 'Luis Angelo Belmonte',
              type: 'qwe',
              dateTimeCreated: new Date(),
              dateTimeUpdated: new Date(),
            },
          ),
        ).to.eventually.rejected;
      });
    });

    describe('GIVEN non existent ID', () => {
      it('should return null', async () => {
        const mock = {
          name: 'Luis Angelo Belmonte',
          type: VendorType.Seamless,
          dateTimeCreated: new Date(),
          dateTimeUpdated: new Date(),
        };
        await insertOneVendor(mock);

        await expect(
          updateVendorByFilters(
            { _id: mockedId },
            {
              name: 'Luis Angelo Belmonte',
              type: VendorType.Transfer,
              dateTimeCreated: new Date(),
              dateTimeUpdated: new Date(),
            },
          ),
        ).to.eventually.fulfilled.and.be.null;
      });
    });
  });

  describe('Deleting a Vendor', () => {
    describe('GIVEN valid and existent vendor ID', () => {
      it('should return true', async () => {
        const mock = {
          name: 'Luis Angelo Belmonte',
          type: VendorType.Seamless,
          dateTimeCreated: new Date(),
          dateTimeUpdated: new Date(),
        };
        const main = await insertOneVendor(mock);

        await expect(deleteOneVendor({ _id: main._id })).to.eventually.fulfilled
          .and.be.true;
      });
    });

    describe('GIVEN non existent vendor ID', () => {
      it('should return true', async () => {
        await expect(deleteOneVendor({ _id: mockedId })).to.eventually.fulfilled
          .and.be.false;
      });
    });

    describe('GIVEN invalid vendor ID', () => {
      it('should return true', async () => {
        await expect(deleteOneVendor({ _id: 'qwe' })).to.eventually.rejected;
      });
    });
  });
});