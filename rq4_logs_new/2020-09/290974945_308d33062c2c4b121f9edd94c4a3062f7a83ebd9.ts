import { getDomain } from './getDomain';
it('should returns the domain of given data with positive values', () => {
    const data = [
        { name: 'FOO', salary: 10000 },
        { name: 'BAZ', salary: 5000 },
    ];
    const domain = getDomain(data);

    expect(domain).toEqual([0, 10000]);
});

it('should returns the domain of given data with negative values', () => {
    const data = [
        { name: 'FOO', salary: -1000 },
        { name: 'BAZ', salary: -50 },
    ];
    const domain = getDomain(data);

    expect(domain).toEqual([-1000, 0]);
});

it('should returns the domain of given data with mixed values', () => {
    const data = [
        { name: 'FOO', salary: 10 },
        { name: 'BAZ', salary: -50 },
    ];
    const domain = getDomain(data);

    expect(domain).toEqual([-50, 10]);
});

it('should returns the default domain ', () => {
    const data = [
        { name: 'FOO', salary: 0 },
        { name: 'BAZ', salary: 0 },
    ];
    const domain = getDomain(data);

    expect(domain).toEqual([0, 0]);
});