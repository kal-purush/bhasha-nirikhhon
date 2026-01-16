export const combineReducers = (reducers: any) => {
  const reducerKeys = Object.keys(reducers);
  const reducerValues = Object.values(reducers);
  let globalState: any;
  reducerKeys.forEach((key, index) => {
    // @ts-ignore
    globalState = { ...globalState, [key]: reducerValues[index][1] };
  });
  let finalReducers = {};
  reducerValues.forEach((value, index) => {
    // @ts-ignore
    finalReducers = { ...finalReducers, [reducerKeys[index]]: value[0] };
  });
  return [
    (state: any, action:any) => {
      let hasStateChanged = false;
      const newState = {};
      let nextStateForCurrentKey = {};
      for (let i = 0; i < reducerKeys.length; i++) {
        const currentKey = reducerKeys[i];
        // @ts-ignore
        const currentReducer = finalReducers[currentKey];
        const prevStateForCurrentKey = state[currentKey];
        nextStateForCurrentKey = currentReducer(prevStateForCurrentKey, action);
        hasStateChanged = hasStateChanged || nextStateForCurrentKey !== prevStateForCurrentKey;
        // @ts-ignore
        newState[currentKey] = nextStateForCurrentKey;
      }
      return hasStateChanged ? newState : state;
    },
    globalState,
  ];
};