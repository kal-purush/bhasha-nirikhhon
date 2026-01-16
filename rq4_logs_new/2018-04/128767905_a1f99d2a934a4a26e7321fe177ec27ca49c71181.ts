import withUniqueNames from './unique-names';

describe('withUniqueNames', () => {

    it('should make names unique and not change the input', () => {
        const input = [{ id: '0', name: 'Foo' }, { id: '1', name: 'Foo' }];
        const output = withUniqueNames(input);
        expect(output).not.toEqual(input);
        expect(output[1]).not.toEqual(input[1]);
        expect(output[1].name).not.toEqual(output[0].name);
        expect(input[1].name).toEqual(input[0].name);
    });
});