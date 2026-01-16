import { testeUnitWithAngular } from './angularUnitTest';

describe('test unit with angular 4', () => {
    it('valider si le nomber egale 100', () => {
        let testUnit: testeUnitWithAngular  = new testeUnitWithAngular();
        let result: number = testUnit.addMethod(5, 5);
        expect(result).toBe(10);
    })
})