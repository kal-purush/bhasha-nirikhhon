export const updateObject = (oldState, updatedState) => {
  return {
    ...oldState,
    ...updatedState,
  };
}