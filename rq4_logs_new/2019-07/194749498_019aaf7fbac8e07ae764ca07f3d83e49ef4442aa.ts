import * as companies from './symbols.json';
import { sanitizeLabel } from '../sanitizers/label-sanitizer';

interface Company {
  label: string;
  id: string;
}

export const createSuggestions = (input: string, limit: number = 10) => {
  const upperCaseInput = input.toUpperCase();
  const suggestions = [];
  for (let company of companies) {
    const { name, symbol, iexId } = company;
    const startsWithInput =
      symbol.startsWith(upperCaseInput) ||
      (name || '').toUpperCase().startsWith(upperCaseInput);
    if (startsWithInput) {
      const suggestion: Company = {
        label: `${sanitizeLabel(name)} (${symbol})`,
        id: iexId,
      };
      suggestions.push(suggestion);
      if (suggestions.length === limit) {
        return suggestions;
      }
    }
  }
  return suggestions;
};