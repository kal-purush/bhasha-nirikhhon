import chai, { expect } from 'chai';

import chaiAsPromised from 'chai-as-promised';

import mongoose from 'mongoose';

import {
  insertVendorUseCase,
  selectAllVendorsUseCase,
  selectOneVendorUseCase,
  updateVendorUseCase,
  deleteOneVendorUseCase,
} from '../../../src/use-cases/vendors';

import { Vendor } from '../../../src/lib/mongoose/models/vendor';

import { initializeDatabase } from '../../../src/lib/mongoose';

chai.use(chaiAsPromised);

const mockedId = mongoose.Types.ObjectId().toString();

describe('Vendor Use Cases', () => {
  before(() => initializeDatabase());

  describe('Adding a Vendor', () => {
    afterEach(() => {
      return Vendor.deleteMany({});
    });

    beforeEach(() => {
      return Vendor.deleteMany({});
    });

    describe('GIVEN correct inputs', () => {
      it('should return the added vendor details', async () => {
        const mock = {
          id: null,
          info: {
            name: 'Luis Angelo Belmonte',
            type: 'SEAMLESS',
          },
          source: null,
        };

        await expect(
          insertVendorUseCase(mock),
        ).to.eventually.fulfilled.and.have.keys('message', 'data');
      });
    });

    describe('GIVEN no name', () => {
      it('should throw an error', async () => {
        const mock = {
          id: null,
          info: {
            name: null,
            type: 'SEAMLESS',
          },
          source: null,
        };

        await expect(insertVendorUseCase(mock)).to.eventually.rejectedWith(
          'Please input name',
        );
      });
    });

    describe('GIVEN no type', () => {
      it('should throw an error', async () => {
        const mock = {
          id: null,
          info: {
            name: 'Luis Angelo Belmonte',
            type: null,
          },
          source: null,
        };

        await expect(insertVendorUseCase(mock)).to.eventually.rejectedWith(
          'Please input type',
        );
      });
    });

    describe('GIVEN invalid type', () => {
      it('should throw an error', async () => {
        const mock = {
          id: null,
          info: {
            name: 'Luis Angelo Belmonte',
            type: 'qwe',
          },
          source: null,
        };

        await expect(insertVendorUseCase(mock)).to.eventually.rejected;
      });
    });

    describe('GIVEN existing vendor name', () => {
      it('should throw an error', async () => {
        const mock = {
          id: null,
          info: {
            name: 'Luis Angelo Belmonte',
            type: 'SEAMLESS',
          },
          source: null,
        };

        const main = {
          id: null,
          info: {
            name: 'Luis Angelo Belmonte',
            type: 'SEAMLESS',
          },
          source: null,
        };

        await expect(
          insertVendorUseCase(mock),
        ).to.eventually.fulfilled.and.have.keys('message', 'data');

        await expect(insertVendorUseCase(main)).to.eventually.rejectedWith(
          'Vendor already exists',
        );
      });
    });
  });

  describe('Selecting All Vendors', () => {
    afterEach(() => {
      return Vendor.deleteMany({});
    });

    beforeEach(() => {
      return Vendor.deleteMany({});
    });

    it('should return list of vendors', async () => {
      const mock = {
        id: null,
        info: {
          name: 'Luis Angelo Belmonte',
          type: 'SEAMLESS',
        },
        source: null,
      };

      await expect(
        insertVendorUseCase(mock),
      ).to.eventually.fulfilled.and.have.keys('message', 'data');

      await expect(
        selectAllVendorsUseCase({ id: null, info: null, source: null }),
      )
        .to.eventually.fulfilled.and.have.keys('message', 'data')
        .and.property('data')
        .have.length(1);
    });
  });

  describe('Selecting One Vendor', () => {
    afterEach(() => {
      return Vendor.deleteMany({});
    });

    beforeEach(() => {
      return Vendor.deleteMany({});
    });

    describe('GIVEN valid ID', () => {
      it('should return the vendor corresponding with the GIVEN ID', async () => {
        const mock = {
          id: null,
          info: {
            name: 'Luis Angelo Belmonte',
            type: 'SEAMLESS',
          },
          source: null,
        };

        const main = await insertVendorUseCase(mock);

        await expect(
          selectOneVendorUseCase({
            id: main.data._id.toString(),
            info: null,
            source: null,
          }),
        ).to.eventually.fulfilled.and.have.keys('message', 'data');
      });
    });

    describe('GIVEN invalid ID', () => {
      it('should throw an error', async () => {
        await expect(
          selectOneVendorUseCase({ id: 'qwe', info: null, source: null }),
        ).to.eventually.rejectedWith('Invalid ID');
      });
    });

    describe('GIVEN valid but not existent ID', () => {
      it('should throw an error', async () => {
        await expect(
          selectOneVendorUseCase({
            id: mockedId,
            info: null,
            source: null,
          }),
        ).to.eventually.rejectedWith("Vendor doesn't exist");
      });
    });
  });

  describe('Updating One Vendor', () => {
    afterEach(() => {
      return Vendor.deleteMany({});
    });

    beforeEach(() => {
      return Vendor.deleteMany({});
    });
    describe('GIVEN valid id and TRANSFER type', () => {
      it('should successfully update to a transfer vendor', async () => {
        const mock = {
          id: null,
          info: {
            name: 'Luis Angelo Belmonte',
            type: 'SEAMLESS',
          },
          source: null,
        };

        const main = await insertVendorUseCase(mock);

        await expect(
          updateVendorUseCase({
            id: main.data._id.toString(),
            info: { name: 'Luis Angelo Belmonte', type: 'TRANSFER' },
            source: null,
          }),
        ).to.eventually.fulfilled.and.have.keys('message', 'data');
      });
    });

    describe('GIVEN valid id and SEAMLESS type', () => {
      it('should successfully update to a seamless vendor', async () => {
        const mock = {
          id: null,
          info: {
            name: 'Luis Angelo Belmonte',
            type: 'TRANSFER',
          },
          source: null,
        };

        const main = await insertVendorUseCase(mock);

        await expect(
          updateVendorUseCase({
            id: main.data._id.toString(),
            info: { name: 'Luis Angelo Belmonte', type: 'SEAMLESS' },
            source: null,
          }),
        ).to.eventually.fulfilled.and.have.keys('message', 'data');
      });
    });

    describe('Given an invalid ID', () => {
      it('should throw an error', async () => {
        await expect(
          updateVendorUseCase({
            id: 'qwe',
            info: { name: 'Luis Angelo Belmonte', type: 'SEAMLESS' },
            source: null,
          }),
        ).to.eventually.rejectedWith('Invalid ID');
      });
    });

    describe('Given an non existent ID', () => {
      it('should throw an error', async () => {
        await expect(
          updateVendorUseCase({
            id: mockedId,
            info: { name: 'Luis Angelo Belmonte', type: 'SEAMLESS' },
            source: null,
          }),
        ).to.eventually.rejectedWith("Vendor ID doesn't exist");
      });
    });

    describe('GIVEN no name', () => {
      it('should throw an error', async () => {
        const mock = {
          id: null,
          info: {
            name: 'Luis Angelo Belmonte',
            type: 'TRANSFER',
          },
          source: null,
        };

        const main = await insertVendorUseCase(mock);

        await expect(
          updateVendorUseCase({
            id: main.data._id.toString(),
            info: { name: null, type: 'SEAMLESS' },
            source: null,
          }),
        ).to.eventually.rejectedWith('Please input name');
      });
    });

    describe('GIVEN no type', () => {
      it('should throw an error', async () => {
        const mock = {
          id: null,
          info: {
            name: 'Luis Angelo Belmonte',
            type: 'TRANSFER',
          },
          source: null,
        };

        const main = await insertVendorUseCase(mock);

        await expect(
          updateVendorUseCase({
            id: main.data._id.toString(),
            info: { name: 'Luis Angelo Belmonte', type: null },
            source: null,
          }),
        ).to.eventually.rejectedWith('Please input type');
      });
    });

    describe('GIVEN an invalid type', () => {
      it('should throw an error', async () => {
        const mock = {
          id: null,
          info: {
            name: 'Luis Angelo Belmonte',
            type: 'TRANSFER',
          },
          source: null,
        };

        const main = await insertVendorUseCase(mock);

        await expect(
          updateVendorUseCase({
            id: main.data._id.toString(),
            info: { name: 'Luis Angelo Belmonte', type: 'qwe' },
            source: null,
          }),
        ).to.eventually.rejected;
      });
    });
  });

  describe('Deleting One Vendor', () => {
    afterEach(() => {
      return Vendor.deleteMany({});
    });

    beforeEach(() => {
      return Vendor.deleteMany({});
    });
    describe('GIVEN a valid ID', () => {
      it('should throw an error', async () => {
        const mock = {
          id: null,
          info: {
            name: 'Luis Angelo Belmonte',
            type: 'TRANSFER',
          },
          source: null,
        };

        const main = await insertVendorUseCase(mock);

        await expect(
          deleteOneVendorUseCase({
            id: main.data._id.toString(),
            source: null,
            info: null,
          }),
        ).to.eventually.fulfilled.and.have.keys('message', 'data');
      });
    });

    describe('GIVEN an invalid ID', () => {
      it('should throw an error', async () => {
        await expect(
          deleteOneVendorUseCase({ id: 'qwe', source: null, info: null }),
        ).to.eventually.rejectedWith('Invalid ID');
      });
    });

    describe('GIVEN a non existent ID', () => {
      it('should throw an error', async () => {
        await expect(
          deleteOneVendorUseCase({ id: mockedId, source: null, info: null }),
        ).to.eventually.rejectedWith("Vendor doesn't exist");
      });
    });
  });
});