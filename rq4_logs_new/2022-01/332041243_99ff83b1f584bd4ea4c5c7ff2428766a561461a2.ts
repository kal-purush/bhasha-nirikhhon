import { TableData, KeyStates } from './interfaces';

const InternalEnum = {
  c5: 'setchaosprice',
  c6: 'setexprice',
  c9: 'chaosprofit',
  c10: 'exprofit',
};

const SortTable = (Table: Array<TableData> = [], SortKey: KeyStates = 'c9', SortType: 0|1 = 1): Array<TableData> => Table.sort((a, b) => {
  // @ts-expect-error im lazy, messing with types later.
  const SortVar = InternalEnum[SortKey];

  // @ts-expect-error im lazy, messing with types later.
  const v1 = a[SortVar];
  // @ts-expect-error im lazy, messing with types later.
  const v2 = b[SortVar];

  if (SortType) return v2 - v1;
  return v1 - v2;
});

export default SortTable;