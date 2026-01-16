import Handler from 'core/handler';
import { BAD_REQUEST, PRECONDITION_FAILED } from 'http-status';
import Joi from 'joi';

/**
 * Return a status code depending the validation error.
 * @param code string
 * @returns number
 */
const getCodes = (code: string): number => {
    const givenCodes = {
        'any.required': PRECONDITION_FAILED,
        'object.unknown': BAD_REQUEST,
    };

    return givenCodes[code] ? givenCodes[code] : BAD_REQUEST;
};

/**
 * Schema verification with Joi.
 * @param requiredFields string[]
 * @param field {}
 * @returns false | string[]
 */
const verifyFields = (
    body: unknown,
    schema: Joi.ObjectSchema<unknown> | Joi.ArraySchema,
): Handler | void => {
    const schemaValidated = schema.validate(body);

    if (schemaValidated.error && schemaValidated.error.details) {
        const field = schemaValidated.error.details[0];

        throw new Handler(
            field.message.replace(/['"]+/g, ''),
            getCodes(field.type),
        );
    }
};

export { getCodes, verifyFields };