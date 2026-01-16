import { describe, expect, it } from 'vitest';
import reducer, { CitiesState, addCity, removeCity } from '../cities';

describe('reducers/cities', () => {
  const initialState: CitiesState = {
    cities: [1],
  };

  describe('addCity', () => {
    it('should add a new city', () => {
      const state = reducer(initialState, addCity(2));

      expect(state).toStrictEqual({
        cities: [1, 2],
      });
    });
  });

  describe('removeCity', () => {
    it('should remove a city', () => {
      const state = reducer(initialState, removeCity(1));

      expect(state).toStrictEqual({
        cities: [],
      });
    });
  });
});