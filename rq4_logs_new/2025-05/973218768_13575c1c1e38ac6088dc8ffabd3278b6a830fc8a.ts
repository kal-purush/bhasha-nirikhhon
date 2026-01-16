export interface ParsedAiText {
  uk: string;
  en: string;
}

const UNNECESSARY_CHARS_REGEX = /[\uFEFF\u00A0]/g;
const MULTIPLE_SPACES_REGEX = /\s\s+/g;
const LEADING_TRAILING_PUNCTUATION_AND_SPACE_REGEX = /^[\s\p{P}]+|[\s\p{P}]+$/gu;

function cleanText(text: string | null | undefined): string {
  if (!text) {
    return '';
  }
  let cleaned = text;
  cleaned = cleaned.replace(UNNECESSARY_CHARS_REGEX, '');
  cleaned = cleaned.replace(MULTIPLE_SPACES_REGEX, ' ');
  cleaned = cleaned.trim();
  cleaned = cleaned.replace(LEADING_TRAILING_PUNCTUATION_AND_SPACE_REGEX, '');
  return cleaned.trim();
}

export function parseAndCleanMultiLanguageText(rawResponse: string): ParsedAiText {
  const result: ParsedAiText = { uk: '', en: '' };

  if (!rawResponse || typeof rawResponse !== 'string') {
    return null;
  }

  const ukTextMatch = rawResponse.match(/---UKRAINIAN_START---([\s\S]*?)---UKRAINIAN_END---/);
  if (ukTextMatch && ukTextMatch[1]) {
    result.uk = cleanText(ukTextMatch[1]);
  }

  if (!ukTextMatch) {
    result.uk = null;
  }

  const enTextMatch = rawResponse.match(/---ENGLISH_START---([\s\S]*?)---ENGLISH_END---/);
  if (enTextMatch && enTextMatch[1]) {
    result.en = cleanText(enTextMatch[1]);
  }

  if (!enTextMatch) {
    result.en = null;
  }

  return result;
}