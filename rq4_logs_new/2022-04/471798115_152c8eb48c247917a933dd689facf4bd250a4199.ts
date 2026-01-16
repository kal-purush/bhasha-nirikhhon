import { supportedNetworks } from '../../constants';
import { getNetworkData } from '../getNetworkData';

describe('getNetworkData', () => {
  test('given supported network name, correct network data object should be returned', () => {
    const result = getNetworkData(supportedNetworks[0].networkName);

    expect(result).toStrictEqual(supportedNetworks[0]);
  });

  test('given supported network name, but with caps letters, correct network data object should be returned', () => {
    const result = getNetworkData(supportedNetworks[1].networkName.toUpperCase());

    expect(result).toStrictEqual(supportedNetworks[1]);
  });

  test('given unsupported network name, null should be returned', () => {
    const result = getNetworkData('lasagna');

    expect(result).toStrictEqual(null);
  });
});