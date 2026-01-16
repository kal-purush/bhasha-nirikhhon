import Handler from 'core/handler';
import {
    NOT_FOUND,
    BAD_REQUEST,
    CONFLICT,
    PRECONDITION_FAILED,
} from 'http-status';

describe('Testing error handling functions', () => {
    test('expect return error calling Handler', () => {
        expect(() => {
            throw new Handler(
                'handling error for testing purpouses',
                NOT_FOUND,
            );
        }).toThrow('handling error for testing purpouses');
        expect.assertions(1);
    });

    test('expect return error message when call Handler.getMessage()', () => {
        const error = new Handler('getting error message', CONFLICT);

        expect(error.getMessage()).toEqual('getting error message');
        expect.assertions(1);
    });

    test('expect return error status_code when call Handler.getStatusCode()', () => {
        const error = new Handler('getting status_code', BAD_REQUEST);

        expect(error.getStatusCode()).toEqual(BAD_REQUEST);
        expect.assertions(1);
    });

    test('expect return error other errors when call Handler.getErrors()', () => {
        const error = new Handler('getting other errors', PRECONDITION_FAILED);

        error.setErrors(['Unexpected token } in JSON at position 2022']);
        expect(error.getErrors()).toEqual([
            'Unexpected token } in JSON at position 2022',
        ]);
        expect.assertions(1);
    });
});