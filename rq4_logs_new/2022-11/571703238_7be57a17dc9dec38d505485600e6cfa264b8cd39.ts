import { classNames } from './classNames';

describe('classNames', () => {
    test('only first param', () => {
        expect(classNames('someClass')).toBe('someClass');
    });
    test('addd param', () => {
        const exp = 'someClass additionalParam class2';
        expect(classNames('someClass', {}, ['additionalParam', 'class2']))
            .toBe(exp);
    });
    test('conditional param', () => {
        const exp = 'someClass additionalParam class2 hovered nigger';
        expect(classNames(
            'someClass',
            { hovered: true, scrollable: false, nigger: true },
            ['additionalParam', 'class2'],
        ))
            .toBe(exp);
    });
    test('conditional param', () => {
        const exp = 'someClass additionalParam class2 nigger';
        expect(classNames(
            'someClass',
            { hovered: undefined, scrollable: false, nigger: true },
            ['additionalParam', 'class2'],
        ))
            .toBe(exp);
    });
});