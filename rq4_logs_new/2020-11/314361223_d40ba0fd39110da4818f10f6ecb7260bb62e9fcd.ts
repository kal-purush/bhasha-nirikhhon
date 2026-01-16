import { piM } from "./pi-m"

describe('pi-m unit test', () => {

    it('Should calculate (127cm = 36.2 kg)', () => {
        // Pattern AAA (Arrange - Act - Assert)

        // Arrange
        let result = 0;

        // Act
        result = piM(127);

        // Assert
        expect(parseFloat(result.toFixed(1))).toBe(36.2);
    })

    it('Should calculate (172cm = 63.2 kg)', () => {
        // Pattern AAA (Arrange - Act - Assert)

        // Arrange
        let result = 0;

        // Act
        result = piM(172);

        // Assert
        expect(parseFloat(result.toFixed(1))).toBe(63.2);
    })

    it('Should calculate (150cm = 50 kg)', () => {
        // Pattern AAA (Arrange - Act - Assert)

        // Arrange
        let result = 0;

        // Act
        result = piM(150);

        // Assert
        expect(parseFloat(result.toFixed(1))).toBe(50);
    })

    it('Should calculate (160cm = 56 kg)', () => {
        // Pattern AAA (Arrange - Act - Assert)

        // Arrange
        let result = 0;

        // Act
        result = piM(160);

        // Assert
        expect(parseFloat(result.toFixed(1))).toBe(56);
    })

    it('Should calculate (140cm = 44 kg)', () => {
        // Pattern AAA (Arrange - Act - Assert)

        // Arrange
        let result = 0;

        // Act
        result = piM(140);

        // Assert
        expect(parseFloat(result.toFixed(1))).toBe(44);
    })

    it('Should calculate (178cm = 66.8 kg)', () => {
        // Pattern AAA (Arrange - Act - Assert)

        // Arrange
        let result = 0;

        // Act
        result = piM(178);

        // Assert
        expect(parseFloat(result.toFixed(1))).toBe(66.8);
    })

    it('Should calculate (120cm = 32 kg)', () => {
        // Pattern AAA (Arrange - Act - Assert)

        // Arrange
        let result = 0;

        // Act
        result = piM(120);

        // Assert
        expect(parseFloat(result.toFixed(1))).toBe(32);
    })
}) 