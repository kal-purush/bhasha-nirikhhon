import chai, { expect } from 'chai';

import chaiAsPromised from 'chai-as-promised';

import mongoose from 'mongoose';

import { Vendor } from '../../../src/lib/mongoose/models/vendor';

import { initializeDatabase } from '../../../src/lib/mongoose';

import {
  selectAllVendorsController,
  selectOneVendorController,
  insertVendorController,
  updateVendorController,
  deleteOneVendorController,
} from '../../../src/controllers/vendors';

chai.use(chaiAsPromised);

const mockedId = mongoose.Types.ObjectId().toString();

describe('Vendor Controller', () => {
  before(() => initializeDatabase());

  describe('Adding a vendor', () => {
    afterEach(() => {
      return Vendor.deleteMany({});
    });

    beforeEach(() => {
      return Vendor.deleteMany({});
    });

    describe('GIVEN correct inputs and SEAMLESS type', () => {
      it('should return a success status code', async () => {
        const main = {
          body: {
            name: 'Luis Angelo Belmonte',
            type: 'SEAMLESS',
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
          insertVendorController(main),
        ).to.eventually.fulfilled.property('statusCode', 201);
      });
    });

    describe('GIVEN correct inputs and TRANSFER type', () => {
      it('should return a success status code', async () => {
        const main = {
          body: {
            name: 'Luis Angelo Belmonte',
            type: 'SEAMLESS',
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
          insertVendorController(main),
        ).to.eventually.fulfilled.property('statusCode', 201);
      });
    });

    describe('GIVEN no name', () => {
      it('should return an error status code', async () => {
        const main = {
          body: {
            name: null,
            type: 'SEAMLESS',
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
          insertVendorController(main),
        ).to.eventually.fulfilled.property('statusCode', 400);
      });
    });

    describe('GIVEN no type', () => {
      it('should return an error status code', async () => {
        const main = {
          body: {
            name: 'Luis Angelo Belmonte',
            type: null,
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
          insertVendorController(main),
        ).to.eventually.fulfilled.property('statusCode', 400);
      });
    });

    describe('GIVEN an invalid type', () => {
      it('should return an error status code', async () => {
        const main = {
          body: {
            name: 'Luis Angelo Belmonte',
            type: 'qwe',
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
          insertVendorController(main),
        ).to.eventually.fulfilled.property('statusCode', 400);
      });
    });

    describe('GIVEN an existing vendor name', () => {
      it('should return an error status code', async () => {
        const mock = {
          body: {
            name: 'Luis Angelo Belmonte',
            type: 'SEAMLESS',
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
          insertVendorController(mock),
        ).to.eventually.fulfilled.property('statusCode', 201);

        await expect(
          insertVendorController(mock),
        ).to.eventually.fulfilled.property('statusCode', 400);
      });
    });
  });

  describe('Selecting all vendors', () => {
    after(() => {
      return Vendor.deleteMany({});
    });

    before(async () => {
      await Vendor.deleteMany({});
      const mock = {
        body: {
          name: 'Luis Angelo Belmonte',
          type: 'SEAMLESS',
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

      await insertVendorController(mock);
    });

    it('should return a success status code', async () => {
      const main = {
        body: null,
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
        selectAllVendorsController(main),
      ).to.eventually.fulfilled.property('statusCode', 200);
    });
  });

  describe('Selecting One Vendor', () => {
    after(() => {
      return Vendor.deleteMany({});
    });

    let mock;

    before(async () => {
      await Vendor.deleteMany({});
      mock = await insertVendorController({
        body: {
          name: 'Luis Angelo Belmonte',
          type: 'SEAMLESS',
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
      });
    });

    describe('GIVEN existent vendor ID', () => {
      it('should return a successfull status code', async () => {
        const main = {
          body: null,
          query: null,
          params: {
            id: mock.body.data._id,
          },
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
          selectOneVendorController(main),
        ).to.eventually.fulfilled.property('statusCode', 200);
      });
    });

    describe('GIVEN non existent vendor ID', () => {
      it('should return an error status code', async () => {
        const main = {
          body: null,
          query: null,
          params: {
            id: mockedId,
          },
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
          selectOneVendorController(main),
        ).to.eventually.fulfilled.property('statusCode', 400);
      });
    });
  });

  describe('Updating a vendor', () => {
    after(() => {
      return Vendor.deleteMany({});
    });

    let mock;

    before(async () => {
      await Vendor.deleteMany({});
      mock = await insertVendorController({
        body: {
          name: 'Luis Angelo Belmonte',
          type: 'SEAMLESS',
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
      });
    });

    describe('GIVEN correct inputs and SEAMLESS type', () => {
      it('should return a successfull status code', async () => {
        const main = {
          body: {
            name: 'Luis Angelo Belmonte',
            type: 'SEAMLESS',
          },
          query: null,
          params: {
            id: mock.body.data._id,
          },
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
          updateVendorController(main),
        ).to.eventually.fulfilled.property('statusCode', 204);
      });
    });

    describe('GIVEN correct inputs and TRANSFER type', () => {
      it('should return a successfull status code', async () => {
        const main = {
          body: {
            name: 'Luis Angelo Belmonte',
            type: 'TRANSFER',
          },
          query: null,
          params: {
            id: mock.body.data._id,
          },
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
          updateVendorController(main),
        ).to.eventually.fulfilled.property('statusCode', 204);
      });
    });

    describe('GIVEN no name', () => {
      it('should return an error status code', async () => {
        const main = {
          body: {
            name: null,
            type: 'TRANSFER',
          },
          query: null,
          params: {
            id: mock.body.data._id,
          },
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
          updateVendorController(main),
        ).to.eventually.fulfilled.property('statusCode', 400);
      });
    });

    describe('GIVEN no type', () => {
      it('should return an error status code', async () => {
        const main = {
          body: {
            name: 'Luis Angelo Belmonte',
            type: null,
          },
          query: null,
          params: {
            id: mock.body.data._id,
          },
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
          updateVendorController(main),
        ).to.eventually.fulfilled.property('statusCode', 400);
      });
    });

    describe('GIVEN invalid type', () => {
      it('should return an error status code', async () => {
        const main = {
          body: {
            name: 'Luis Angelo Belmonte',
            type: 'qwe',
          },
          query: null,
          params: {
            id: mock.body.data._id,
          },
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
          updateVendorController(main),
        ).to.eventually.fulfilled.property('statusCode', 400);
      });
    });
  });

  describe('Deleting a vendor', () => {
    after(() => {
      return Vendor.deleteMany({});
    });

    let mock;

    before(async () => {
      await Vendor.deleteMany({});
      mock = await insertVendorController({
        body: {
          name: 'Luis Angelo Belmonte',
          type: 'SEAMLESS',
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
      });
    });

    describe('GIVEN an existent vendor ID', () => {
      it('should return a successfull status code', async () => {
        const main = {
          body: null,
          query: null,
          params: {
            id: mock.body.data._id,
          },
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
          deleteOneVendorController(main),
        ).to.eventually.fulfilled.property('statusCode', 200);
      });
    });

    describe('GIVEN a non existent vendor ID', () => {
      it('should return a successfull status code', async () => {
        const main = {
          body: null,
          query: null,
          params: {
            id: mockedId,
          },
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
          deleteOneVendorController(main),
        ).to.eventually.fulfilled.property('statusCode', 400);
      });
    });
  });
});