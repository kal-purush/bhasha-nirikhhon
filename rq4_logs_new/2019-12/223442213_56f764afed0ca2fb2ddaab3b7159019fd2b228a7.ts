import { NormalizedBitsState } from '../interfaces/bits';

// Helper function which recursively checks the child bits
// and sets a new isComplete state on them
export const setCompletionStateForChildren = (
  draft: NormalizedBitsState,
  bId: string,
  completionState?: boolean,
) => {
  // Find the children if there are any
  const children = Object.values(draft.bits).filter(b =>
    draft.bits[bId].bits.includes(b._id),
  );

  // Set their isComplete state and search for their children
  children.forEach(child => {
    child.isComplete = completionState;
    setCompletionStateForChildren(draft, child._id, completionState);
  });
};