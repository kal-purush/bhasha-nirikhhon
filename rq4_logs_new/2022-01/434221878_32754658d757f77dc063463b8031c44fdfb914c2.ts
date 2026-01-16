// except.ts

/**
 * Basic class for custom application exceptions. Redefines the 'name' property
 * to return the actual class name of the exception. Keeps the status code which
 * can be use as the process exit status. The supported status codes are listed
 * as the static variables of the class.
 * TODO: consider if TS enums can be harnessed for the latter.
 */
export class ApplicationException extends Error {
  statusCode: number;

  constructor(message: string, statusCode: number /* TODO: enum */) {
    super(message);
    this.statusCode = statusCode;
  }

  get name() {
    return this.constructor.name;
  }

  // ? TS enum instead of the static properties ?
  static readonly SERVER_START_ERROR = 1;

  static readonly DATABASE_CONNECTION_ERROR = 2;

  static readonly GENERAL_APPLICATION_ERROR = 3;

  static readonly UNHANDLED_EXCEPTION = 126;

  static readonly UNHANDLED_REJECTION = 125;

  static readonly UNEXPECTED = 0xff;
}

// __EOF__