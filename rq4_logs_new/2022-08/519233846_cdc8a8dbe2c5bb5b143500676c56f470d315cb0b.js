const ResponseCodes = {
  SUCCESS: {
    OK: 200,
    USER_CREATED: 201,
  },
  ERROR: {
    INVALID_DATA: 400,
    NOT_AUTHORIZED: 401,
    NOT_FOUND: 404,
  },
  SERVER: {
    INTERNAL_ERROR: 500,
  },
};

module.exports = {
  ResponseCodes,
};