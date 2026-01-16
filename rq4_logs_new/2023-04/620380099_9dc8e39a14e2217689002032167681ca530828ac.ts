import { StatusCodes } from "../types/status-codes.enum";

class BaseError extends Error {
  constructor(message: string) {
    super(message)
  }
}

export class ValidationError extends BaseError {
  private readonly code: number;
  constructor(message: string) {
    super(message);
    this.code = StatusCodes.BAD_REQUEST;
    this.name = this.constructor.name;
  }
}

export class ForbiddenError extends BaseError {
  private readonly code: number;
  constructor(message: string) {
    super(message);
    this.code = StatusCodes.FORBIDDEN;
    this.name = this.constructor.name;
  }
}

export class NotFoundError extends BaseError {
  private readonly code: number;
  constructor(message: string) {
    super(message);
    this.code = StatusCodes.NOT_FOUND;
    this.name = this.constructor.name;
  }
}