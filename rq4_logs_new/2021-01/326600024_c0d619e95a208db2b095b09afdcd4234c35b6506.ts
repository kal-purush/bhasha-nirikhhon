import chai, { expect } from 'chai';

import chaiAsPromised from 'chai-as-promised';

import { vendorEntity } from '../../src/entities/vendor';

chai.use(chaiAsPromised);

describe('test', () => {
  it('return the vendor object', async () => {
    const mock = {
      name: 'Luis Angelo Belmonte',
      type: 'SEAMLESS',
      dateTimeCreated: new Date(),
      dateTimeUpdated: new Date(),
    };

    await expect(vendorEntity(mock)).to.eventually.fulfilled.and.eqls(mock);
  });

  it('throw an error if no name is provided', async () => {
    const mock = {
      name: '',
      type: 'SEAMLESS',
      dateTimeCreated: new Date(),
      dateTimeUpdated: new Date(),
    };

    await expect(vendorEntity(mock)).to.eventually.rejectedWith(
      'Please input name',
    );
  });

  it('throw an error if no type is provided', async () => {
    const mock = {
      name: 'Luis Angelo Belmonte',
      type: '',
      dateTimeCreated: new Date(),
      dateTimeUpdated: new Date(),
    };

    await expect(vendorEntity(mock)).to.eventually.rejectedWith(
      'Please input type',
    );
  });
});