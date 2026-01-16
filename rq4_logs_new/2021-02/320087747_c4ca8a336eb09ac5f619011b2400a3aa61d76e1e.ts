import { makeQueryString, parseGetParams } from '../../../utils/UrlUtils';

fdescribe('UrlUtils', () => {
    describe('parseGetParams', () => {
        describe('when called with key and value pairs', () => {
            const format = 'test=one&foo=two';
            let result: Object;

            beforeEach(() => {
                result = parseGetParams(format);
            });

            test('should parse both keys and values', () => {
                expect(result).toEqual({
                    test: 'one',
                    foo: 'two'
                });
            });
        });
    
        describe('when called with keys but no values', () => {
            const format = 'test&foo';
    
            describe('parseGetParams', () => {
                let result: Object;
                beforeEach(() => {
                    result = parseGetParams(format);
                });
    
                test('should parse the keys, but not the values', () => {
                    expect(result).toEqual({
                        test: undefined,
                        foo: undefined
                    });
                });
            });
        });
    
        describe('when called with mix of keys and values', () => {
            const format = 'test&foo=bar';
            let result: Object;

            beforeEach(() => {
                result = parseGetParams(format);
            });

            test('should parse results as provided', () => {
                expect(result).toEqual({
                    test: undefined,
                    foo: 'bar'
                });
            });
        });
    });

    describe('makeQueryString', () => {
        describe('called with simple object', () => {
            const input = {
                foo: 'bar',
                baz: 'baz'
            };
            let output: string;

            beforeEach(() => {
                output = makeQueryString(input);
            });

            test('should convert to url string', () => {
                expect(output).toBe('foo=bar&baz=baz');
            });
        });
    });
});