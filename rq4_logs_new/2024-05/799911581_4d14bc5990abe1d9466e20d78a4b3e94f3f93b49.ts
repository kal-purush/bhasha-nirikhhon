const specialChars = '!@#$%^&*<>()-+=,';

const charToStringMap = new Map([
  ['!', '_bang_'],
  ['@', '_at_'],
  ['#', '_hash_'],
  ['$', '_dollar_'],
  ['%', '_percent_'],
  [',', '_comma_'],
  ['^', '_caret_'],
  ['&', '_and_'],
  ['*', '_asterisk_'],
  ['(', '_lparen_'],
  [')', '_rparen_'],
  ['<', '_langle_'],
  ['>', '_rangle_'],
  ['-', '_hyphen_'],
  ['+', '_plus_'],
  ['=', '_equals_'],
]);

export const normalizeString = (original: string | null): string | null => {
  if (!original) {
    return null;
  }

  const normalized = original
    .split('')
    .map((x) => {
      if (specialChars.includes(x)) {
        return charToStringMap.get(x);
      }

      return x;
    })
    .join('');
  return normalized.toLowerCase();
};