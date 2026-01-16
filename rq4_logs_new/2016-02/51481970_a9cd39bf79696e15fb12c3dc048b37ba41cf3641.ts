import LongPathPerson from '../../../tests/mocks/LongPathPerson';

describe('Person', () => {
    it('should report name', () => {
        expect(new LongPathPerson('John-imported-from-Long-Path-directory').name).to.equal('John-imported-from-Long-Path-directory');
    });
});