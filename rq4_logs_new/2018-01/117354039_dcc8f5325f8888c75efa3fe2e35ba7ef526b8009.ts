import ExtendoError from "extendo-error";
import { PlorthErrorCode, PlorthValueType } from "plorth-types";

function errorCodeToString(code: PlorthErrorCode): string {
  switch (code) {
    case PlorthErrorCode.SYNTAX:
      return "Syntax error";

    case PlorthErrorCode.REFERENCE:
      return "Reference error";

    case PlorthErrorCode.TYPE:
      return "Type error";

    case PlorthErrorCode.VALUE:
      return "Value error";

    case PlorthErrorCode.RANGE:
      return "Range error";

    case PlorthErrorCode.IMPORT:
      return "Import error";

    default:
      return "Unknown error";
  }
}

function constructErrorMessage(code: PlorthErrorCode, message?: string): string {
  let result = errorCodeToString(code);

  if (message) {
    result += `: ${message}`;
  }

  return result;
}

export default class RuntimeError extends ExtendoError {
  type: PlorthValueType.ERROR;
  code: PlorthErrorCode;

  constructor(code: PlorthErrorCode, message?: string) {
    super(constructErrorMessage(code, message));
    this.code = code;
  }
}