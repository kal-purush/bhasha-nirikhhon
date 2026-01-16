import { search } from '../index';

it('must return query', () => {
  expect(search('query')).toBe('query');
});