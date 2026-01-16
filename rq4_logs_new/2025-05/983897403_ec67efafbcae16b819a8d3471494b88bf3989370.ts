import { NumerParseResult } from "@/interfaces";

export const parseCounterValue = (
  finalNumber: string | number
): NumerParseResult => {
  if (typeof finalNumber === "number") {
    return {
      isNumber: true,
      numericValue: finalNumber,
      originalValue: finalNumber,
    };
  }

  const stringValue = finalNumber.toString();

  const match = stringValue.match(/^([^0-9]*)?([0-9]+(?:\.[0-9]+)?)(.*)?$/);

  if (match) {
    const [, prefixSymbol = "", numberPart, suffixSymbol = ""] = match;
    const numericValue = parseFloat(numberPart);
    const symbol = (prefixSymbol + suffixSymbol).trim();
    return {
      isNumber: false,
      numericValue,
      symbol: symbol || undefined,
      originalValue: finalNumber,
    };
  }

  return {
    isNumber: false,
    numericValue: 0,
    symbol: stringValue,
    originalValue: finalNumber,
  };
};