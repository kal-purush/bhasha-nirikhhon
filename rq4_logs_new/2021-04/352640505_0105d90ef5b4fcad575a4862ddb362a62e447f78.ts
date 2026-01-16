import formatAnalysis from './formatAnalysis';

test('formats "select_unique" analysis', () => {
  expect(formatAnalysis('select_unique')).toEqual('Select Unique');
});

test('formats "extraction" analysis', () => {
  expect(formatAnalysis('extraction')).toEqual('Extraction');
});