export const monetaryFormat = (value: number, decimal: number): string => {
  return Number(value).toFixed(decimal).replace(',', '').replace('.', ',');
};