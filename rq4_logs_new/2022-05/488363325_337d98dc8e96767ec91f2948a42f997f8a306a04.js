"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.BcResultSchema = exports.BcQueryStringSchema = void 0;
//setup the schema structure to enforce the query parameters, also used for swagger generatieon 
exports.BcQueryStringSchema = {
    type: 'object',
    properties: {
        q: { type: 'string', description: 'query string' },
        p: { type: 'string', description: 'parent block' },
        b: { type: 'string', description: 'block id' },
        x: { type: 'string', default: '000', description: 'complexity - starts with' },
        m: { type: 'number', default: 1000000, description: 'maximum number of tries' },
    },
    required: ['q', 'p', 'b'],
};
exports.BcResultSchema = {
    type: 'object',
    properties: {
        blockHash: { type: 'string' },
        blockId: { type: 'string' },
        executionTimeMs: { type: 'number' },
        found: { type: 'boolean' },
        nonce: { type: 'number' },
        parentHash: { type: 'string' },
        query: { type: 'string' },
    },
    required: ['blockHash', 'blockId', 'executionTimeMs', 'found', 'nonce', 'parentHash', 'query'],
};
//# sourceMappingURL=schemas.js.map