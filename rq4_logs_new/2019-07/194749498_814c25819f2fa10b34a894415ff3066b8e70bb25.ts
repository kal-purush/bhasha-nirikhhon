import { getSuggestionOrSearchText, Suggestion } from '../src/util';

const emptyMockSuggestions: Suggestion[] = [];
const mockSuggestions: Suggestion[] = [
  { symbol: 'patate', id: '0', label: '' },
  { symbol: 'lmao', id: '1', label: '' },
];

test('Succesfully return undefined if provided an empty string and no suggestions', () => {
  expect(getSuggestionOrSearchText(emptyMockSuggestions, '')).toBe(undefined);
});

test('Succesfully return undefined if provided an empty string with suggestions', () => {
  expect(getSuggestionOrSearchText(mockSuggestions, '')).toBe(undefined);
});

test('Succesfully return the first symbol if provided a string and suggestions', () => {
  expect(getSuggestionOrSearchText(mockSuggestions, 'lmao')).toBe('patate');
});

test('Succesfully return the symbol if there are no suggestions', () => {
  expect(getSuggestionOrSearchText(emptyMockSuggestions, '(lmao)')).toBe(
    '(lmao)'
  );
});

test('Succesfully return the first suggestion symbol if provided a symbol as a search string and suggestions', () => {
  expect(getSuggestionOrSearchText(mockSuggestions, '(lmao)')).toBe('patate');
});