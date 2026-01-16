import { combineReducers } from '@reduxjs/toolkit';

import { carouselSlice } from '../features/carousel/carouselSlice';
import { dataSlice } from '../features/data/dataSlice';

const rootReducer = combineReducers({
  data: dataSlice.reducer,
  currentPerson: carouselSlice.reducer,
});

export type RootState = ReturnType<typeof rootReducer>;
export default rootReducer;