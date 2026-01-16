import { WeatherProvider } from '../index';

test('My Weather Provider', () => {
  expect(WeatherProvider('Carl')).toBe('Weather Carl');
});