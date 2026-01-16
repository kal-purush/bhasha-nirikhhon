import { promiseToObservable, getAsObservable } from './APICall';

test('promiseToObservable calls the function with the right argument', () => {
  const fn = jest.fn();
  promiseToObservable(fn, 5);
  expect(fn.mock.calls.length).toBe(1);
  expect(fn.mock.calls[0][0]).toBe(5);
});

test('getAsObservable calls the functions with right arguments', () => {
  const axios = jest.fn();
  const promiseToObservable = jest.fn();
  getAsObservable(axios, promiseToObservable, 'a.fr', { a: 1 });
  expect(axios.mock.calls.length).toBe(1);
  expect(axios.mock.calls[0][0]).toBe('a.fr');
  expect(axios.mock.calls[0][1]).toEqual({ a: 1 });
});