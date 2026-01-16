import { NormalizedBit } from '../interfaces/bits';

export const findNextBit = (bits: NormalizedBit[]): string => {
  const incompleteBits = bits.filter(b => !b.isComplete);
  const bit = incompleteBits[0];
  const hasChildren = bit.bits.length > 0;
  if (hasChildren) {
    const children = bits.filter(b => bit.bits.includes(b._id));
    const incompleteChildren = children.filter(b => !b.isComplete);
    if (incompleteChildren.length > 0) {
      return findNextBit(incompleteChildren);
    }
    console.log('INCOMPLETE CHILD INDEX ISSUSE HIT');
    return '';
  } else {
    return bit._id;
  }
};