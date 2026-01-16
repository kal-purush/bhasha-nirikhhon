// const ResponseStatusCode = require('../../config/responseStatusCode')
const ResponseStatusCode = require('../class/ResponseStatusCode')

class AppError extends Error {
  constructor(statusCode, message) {
    super(message);
    this.statusCode = statusCode;
  }

  static badRequest(message) {
    return new AppError(ResponseStatusCode.BAD_REQUEST, message);
  }

  static notAcceptable(message) {
    return new AppError(ResponseStatusCode.NOT_ACCEPTABLE, message);
  }

  static unauthorized(message) {
    return new AppError(ResponseStatusCode.UNAUTHORIZED, "User Unauthorized");
  }

  static forbidden(message) {
    return new AppError(ResponseStatusCode.FORBIDDEN, message);
  }

  static dataNotFound(message) {
    return new AppError(ResponseStatusCode.DATA_NOT_FOUND_ERROR, message);
  }

  static internal(message) {
    return new AppError(ResponseStatusCode.INTERNAL_SERVER_ERROR, message);
  }
}

module.exports = AppError;