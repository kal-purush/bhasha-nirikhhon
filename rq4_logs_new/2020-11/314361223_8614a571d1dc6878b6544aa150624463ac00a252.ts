import { imc } from "./imc-h"

describe('imc-h unit test', () => {

    it('Should calculate', () => {
        // Pattern AAA (Arrange - Act - Assert)

        // Arrange
        let result = 0;

        // Act
        result = imc(29.7,127);

        // Assert
        expect(parseFloat(result.toFixed(1))).toBe(18.4);
    })

    it('Should calculate', () => {
        // Pattern AAA (Arrange - Act - Assert)

        // Arrange
        let result = 0;

        // Act
        result = imc(59.2,172);

        // Assert
        expect(parseFloat(result.toFixed(1))).toBe(20);
    })

    it('Should calculate', () => {
        // Pattern AAA (Arrange - Act - Assert)

        // Arrange
        let result = 0;

        // Act
        result = imc(60,150);

        // Assert
        expect(parseFloat(result.toFixed(1))).toBe(26.7);
    })

    it('Should calculate', () => {
        // Pattern AAA (Arrange - Act - Assert)

        // Arrange
        let result = 0;

        // Act
        result = imc(70,160);

        // Assert
        expect(parseFloat(result.toFixed(1))).toBe(27.3);
    })

    it('Should calculate', () => {
        // Pattern AAA (Arrange - Act - Assert)

        // Arrange
        let result = 0;

        // Act
        result = imc(60,140);

        // Assert
        expect(parseFloat(result.toFixed(1))).toBe(30.6);
    })

    it('Should calculate', () => {
        // Pattern AAA (Arrange - Act - Assert)

        // Arrange
        let result = 0;

        // Act
        result = imc(90,160);

        // Assert
        expect(parseFloat(result.toFixed(1))).toBe(35.2);
    })

    it('Should calculate', () => {
        // Pattern AAA (Arrange - Act - Assert)

        // Arrange
        let result = 0;

        // Act
        result = imc(130,178);

        // Assert
        expect(parseFloat(result.toFixed(1))).toBe(41);
    })

    it('Should calculate', () => {
        // Pattern AAA (Arrange - Act - Assert)

        // Arrange
        let result = 0;

        // Act
        result = imc(75,120);

        // Assert
        expect(parseFloat(result.toFixed(1))).toBe(52.1);
    })
}) 