class CustomError extends Error {
  public readonly action: string;
  public readonly statusCode: number;
  public readonly isPublicError: boolean;

  constructor(message: string, cause: unknown, action: string, statusCode: number, isPublicError: boolean) {
    super(message, { cause: cause });
    this.action = action;
    this.statusCode = statusCode;
    this.isPublicError = isPublicError;
  }
}

export class UserValidationsError extends CustomError {
  constructor(message: string, cause: unknown, action: string, statusCode: number, isPublicError: boolean) {
    super(message, cause, action, statusCode, isPublicError);
    this.name = "UserValidationsError2";
  }
}