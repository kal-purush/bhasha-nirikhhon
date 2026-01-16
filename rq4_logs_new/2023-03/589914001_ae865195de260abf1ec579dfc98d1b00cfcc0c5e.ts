import { ServiceErrorCode } from '@lentovaraukset/shared/src';

export default class ServiceError extends Error {
  errorCode: ServiceErrorCode;

  constructor(errorCode: ServiceErrorCode, message?: string) {
    super(message);
    this.name = this.constructor.name;

    this.errorCode = errorCode;

    // https://github.com/microsoft/TypeScript/issues/13965
    // https://stackoverflow.com/questions/31626231/custom-error-class-in-typescript
    // instanceof not working fix:
    Object.setPrototypeOf(this, ServiceError.prototype);
  }
}

export const isSpecificServiceError = (
  possibleError: unknown,
  errorCode: ServiceErrorCode,
): boolean => {
  if (!(possibleError instanceof ServiceError)) { return false; }
  if (possibleError.errorCode !== errorCode) { return false; }
  return true;
};