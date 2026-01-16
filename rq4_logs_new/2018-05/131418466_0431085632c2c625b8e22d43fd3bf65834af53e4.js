'use strict';
exports.DATABASE_URL = process.env.DATABASE_URL || 'mongod://localhost/posts';
exports.TEST_DATABASE_URL = process.env.TEST_DATABASE_URL || 'mongod://localhost/test-posts';
exports.PORT = process.env.PORT || 8080;