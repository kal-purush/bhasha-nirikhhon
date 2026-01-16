import { clasificacion } from "./clasificacion-seedo-h"

describe('clasificacion unit tests', () => {
    
    it('Should clasificate imc 18.4 = desnutricion', () => {
        // Pattern AAA (Arrange - Act - Assert)

        // Arrange
        let result;
        
        // Act
        result = clasificacion(18.4);

        // Assert
        expect(result).toBe('Desnutricion');
    })

    it('Should clasificate imc 19 = Normal', () => {
        // Pattern AAA (Arrange - Act - Assert)

        // Arrange
        let result;
        
        // Act
        result = clasificacion(19);

        // Assert
        expect(result).toBe('Normal');
    })

    it('Should clasificate imc 25 = Sobre peso grado 1', () => {
        // Pattern AAA (Arrange - Act - Assert)

        // Arrange
        let result;
        
        // Act
        result = clasificacion(25);

        // Assert
        expect(result).toBe('Sobre peso grado 1');
    })


    it('Should clasificate imc 27 = Sobre peso grado 2', () => {
        // Pattern AAA (Arrange - Act - Assert)

        // Arrange
        let result;
        
        // Act
        result = clasificacion(27);

        // Assert
        expect(result).toBe('Sobre peso grado 2');
    })

    it('Should clasificate imc 30 = Obesidad tipo 1', () => {
        // Pattern AAA (Arrange - Act - Assert)

        // Arrange
        let result;
        
        // Act
        result = clasificacion(30);

        // Assert
        expect(result).toBe('Obesidad tipo 1');
    })
    
    it('Should clasificate imc 35 = Obesidad tipo 2', () => {
        // Pattern AAA (Arrange - Act - Assert)

        // Arrange
        let result;
        
        // Act
        result = clasificacion(35);

        // Assert
        expect(result).toBe('Obesidad tipo 2');
    })

    it('Should clasificate imc 40 = Obesidad tipo 3 Morbida', () => {
        // Pattern AAA (Arrange - Act - Assert)

        // Arrange
        let result;
        
        // Act
        result = clasificacion(40);

        // Assert
        expect(result).toBe('Obesidad tipo 3 Morbida');
    })

    it('Should clasificate imc 50 = Obesidad tipo 4 Extrema', () => {
        // Pattern AAA (Arrange - Act - Assert)

        // Arrange
        let result;
        
        // Act
        result = clasificacion(50);

        // Assert
        expect(result).toBe('Obesidad tipo 4 Extrema');
    
})
})