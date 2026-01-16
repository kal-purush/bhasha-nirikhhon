import { piH } from "./pi-h"

describe('pi-h unit test', () => {

    it('Should calculate (127cm = 32.8 kg)', () => {
        // Pattern AAA (Arrange - Act - Assert)

        // Arrange
        let result = 0;

        // Act
        result = piH(127);

        // Assert
        expect(parseFloat(result.toFixed(1))).toBe(32.8);
    })

    it('Should calculate (172cm = 66.5 kg)', () => {
        // Pattern AAA (Arrange - Act - Assert)

        // Arrange
        let result = 0;

        // Act
        result = piH(172);

        // Assert
        expect(parseFloat(result.toFixed(1))).toBe(66.5);
    })

    it('Should calculate (150cm = 50 kg)', () => {
        // Pattern AAA (Arrange - Act - Assert)

        // Arrange
        let result = 0;

        // Act
        result = piH(150);

        // Assert
        expect(parseFloat(result.toFixed(1))).toBe(50);
    })

    it('Should calculate (160cm = 57.5 kg)', () => {
        // Pattern AAA (Arrange - Act - Assert)

        // Arrange
        let result = 0;

        // Act
        result = piH(160);

        // Assert
        expect(parseFloat(result.toFixed(1))).toBe(57.5);
    })

    it('Should calculate (140cm = 42.5 kg)', () => {
        // Pattern AAA (Arrange - Act - Assert)

        // Arrange
        let result = 0;

        // Act
        result = piH(140);

        // Assert
        expect(parseFloat(result.toFixed(1))).toBe(42.5);
    })

    it('Should calculate (178cm = 71 kg)', () => {
        // Pattern AAA (Arrange - Act - Assert)

        // Arrange
        let result = 0;

        // Act
        result = piH(178);

        // Assert
        expect(parseFloat(result.toFixed(1))).toBe(71);
    })

    it('Should calculate (120cm = 27.5 kg)', () => {
        // Pattern AAA (Arrange - Act - Assert)

        // Arrange
        let result = 0;

        // Act
        result = piH(120);

        // Assert
        expect(parseFloat(result.toFixed(1))).toBe(27.5);
    })
}) 