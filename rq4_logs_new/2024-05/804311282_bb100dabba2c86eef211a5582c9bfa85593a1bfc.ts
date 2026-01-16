import { ITransaction } from '../../../src/types';

export const mockResults: ITransaction[] = [
  {
    from: 'sender1',
    to: 'receiver1',
    blockNumber: '1',
    blockHash: 'hash1',
    hash: 'hash1',
    gasPrice: '100',
    value: '10',
  },
  {
    from: 'sender2',
    to: 'receiver2',
    blockNumber: '2',
    blockHash: 'hash2',
    hash: 'hash2',
    gasPrice: '200',
    value: '20',
  },
  {
    from: 'sender3',
    to: 'receiver3',
    blockNumber: '3',
    blockHash: 'hash3',
    hash: 'hash3',
    gasPrice: '300',
    value: '30',
  },
  {
    from: 'sender4',
    to: 'receiver4',
    blockNumber: '4',
    blockHash: 'hash4',
    hash: 'hash4',
    gasPrice: '400',
    value: '40',
  },
  {
    from: 'sender5',
    to: 'receiver5',
    blockNumber: '5',
    blockHash: 'hash5',
    hash: 'hash5',
    gasPrice: '500',
    value: '50',
  },
];