using Fractions;
using Xunit;

namespace OSK.Math.Polynomials.UnitTests
{
    public class PolynomialMathTests
    {
        #region Multiplication

        [Fact]
        public void Polynomial_FractionMultiplication_ReturnsPolynomialMultipliedByTerm()
        {
            // Arrange
            var polynomialA = new Polynomial(2, 3);
            var fraction = new Fraction(2, 1);

            // Act
            var result = PolynomialMath.Multiply(polynomialA, fraction);

            // Assert

            // (2 + 3x) * 2 = 4 + 6x
            Assert.Equal(1, result.Degree);
            Assert.Equal(2, result.Length);

            Assert.Equal(4, result[0].Coeffecient);
            Assert.Equal(0, result[0].Exponent);

            Assert.Equal(6, result[1].Coeffecient);
            Assert.Equal(1, result[1].Exponent);
        }

        [Fact]
        public void Polynomial_PolynomialTermMultiplication_ReturnsPolynomialMultipliedByTerm()
        {
            // Arrange
            var polynomialA = new Polynomial(2, 3);
            var polynomialTerm = new PolynomialTerm(2, 1, 3);

            // Act
            var result = PolynomialMath.Multiply(polynomialA, polynomialTerm);

            // Assert

            // (2 + 3x) * 2x^3 = 4x^3 + 6x^4
            Assert.Equal(4, result.Degree);
            Assert.Equal(2, result.Length);

            Assert.Equal(4, result[0].Coeffecient);
            Assert.Equal(3, result[0].Exponent);

            Assert.Equal(6, result[1].Coeffecient);
            Assert.Equal(4, result[1].Exponent);
        }

        [Fact]
        public void Polynomial_PolynomialMultiplication_ReturnsPolynomialsMultiplied()
        {
            // Arrange
            var polynomialA = new Polynomial(2, 3);
            var polynomialB = new Polynomial(3, 4);

            // Act
            var result = PolynomialMath.Multiply(polynomialA, polynomialB);

            // Assert

            // (2 + 3x) * (3 + 4x) = 6 + 17x + 12x^2
            Assert.Equal(2, result.Degree);
            Assert.Equal(3, result.Length);

            Assert.Equal(6, result[0].Coeffecient);
            Assert.Equal(0, result[0].Exponent);

            Assert.Equal(17, result[1].Coeffecient);
            Assert.Equal(1, result[1].Exponent);

            Assert.Equal(12, result[2].Coeffecient);
            Assert.Equal(2, result[2].Exponent);
        }

        #endregion

        #region Division

        [Fact]
        public void Polynomial_FractionDivision_ReturnsPolynomialDivided()
        {
            // Arrange
            var polynomialA = new Polynomial(1, 2, 1);
            var fraction = new Fraction(2, 1);

            // Act
            var result = PolynomialMath.Divide(polynomialA, fraction);

            // Assert

            // (x^2 + 2x + 1) / 2 = 1/2x^2 + x + 1/2

            Assert.Equal(2, result.Degree);
            Assert.Equal(3, result.Length);

            Assert.Equal(new Fraction(1, 2), result[0].Coeffecient);
            Assert.Equal(0, result[0].Exponent);

            Assert.Equal(1, result[1].Coeffecient);
            Assert.Equal(1, result[1].Exponent);

            Assert.Equal(new Fraction(1, 2), result[2].Coeffecient);
            Assert.Equal(2, result[2].Exponent);
        }


        [Fact]
        public void Polynomial_PolynomialTermDivision_ReturnsPolynomialDivided()
        {
            // Arrange
            var polynomialA = new Polynomial(1, 2, 1);
            var polynomialTerm = new PolynomialTerm(2, 1, 1);

            // Act
            var result = PolynomialMath.Divide(polynomialA, polynomialTerm);

            // Assert

            // (x^2 + 2x + 1) / (2x) = 1/2x + 1 rem. 1

            Assert.Equal(1, result.Degree);
            Assert.Equal(2, result.Length);

            Assert.Equal(1, result[0].Coeffecient);
            Assert.Equal(0, result[0].Exponent);

            Assert.Equal(new Fraction(1, 2), result[1].Coeffecient);
            Assert.Equal(1, result[1].Exponent);
        }

        [Fact]
        public void Polynomial_PolynomialDivision_ReturnsPolynomialsDivided()
        {
            // Arrange
            var polynomialA = new Polynomial(1, 2, 1);
            var polynomialB = new Polynomial(2, 1);

            // Act
            var result = PolynomialMath.Divide(polynomialA, polynomialB);

            // Assert

            // (x^2 + 2x + 1) / (x + 2) = x rem. 1 

            var quotient = result.Quotient;
            Assert.Equal(1, quotient.Degree);
            Assert.Equal(1, quotient.Length);
            Assert.Equal(1, quotient[0].Coeffecient);
            Assert.Equal(1, quotient[0].Exponent);

            var remainder = result.Remainder;
            Assert.Equal(0, remainder.Degree);
            Assert.Equal(1, remainder.Length);
            Assert.Equal(1, remainder[0].Coeffecient);
            Assert.Equal(0, remainder[0].Exponent);
        }

        #endregion

        #region Addition

        [Fact]
        public void Polynomial_FractionAddition_ReturnsPolynomialAdded()
        {
            // Arrange
            var polynomialA = new Polynomial(2, 3, 5, 3);
            var fraction = new Fraction(2, 1);

            // Act
            var result = PolynomialMath.Add(polynomialA, fraction);

            // Assert

            // (2 + 3x + 5x^2 + 3x^3) + (2) = 4 + 3x + 5x^2 + 3x^3 
            Assert.Equal(3, result.Degree);
            Assert.Equal(4, result.Length);

            Assert.Equal(4, result[0].Coeffecient);
            Assert.Equal(0, result[0].Exponent);

            Assert.Equal(3, result[1].Coeffecient);
            Assert.Equal(1, result[1].Exponent);

            Assert.Equal(5, result[2].Coeffecient);
            Assert.Equal(2, result[2].Exponent);

            Assert.Equal(3, result[3].Coeffecient);
            Assert.Equal(3, result[3].Exponent);
        }

        [Fact]
        public void Polynomial_PolynomialTermAddition_ReturnsPolynomialAdded()
        {
            // Arrange
            var polynomialA = new Polynomial(2, 3, 5, 3);
            var polynomialTerm = new PolynomialTerm(2, 1, 2);

            // Act
            var result = PolynomialMath.Add(polynomialA, polynomialTerm);

            // Assert

            // (2 + 3x + 5x^2 + 3x^3) + (2x^2) = 2 + 3x + 7x^2 + 3x^3 
            Assert.Equal(3, result.Degree);
            Assert.Equal(4, result.Length);

            Assert.Equal(2, result[0].Coeffecient);
            Assert.Equal(0, result[0].Exponent);

            Assert.Equal(3, result[1].Coeffecient);
            Assert.Equal(1, result[1].Exponent);

            Assert.Equal(7, result[2].Coeffecient);
            Assert.Equal(2, result[2].Exponent);

            Assert.Equal(3, result[3].Coeffecient);
            Assert.Equal(3, result[3].Exponent);
        }

        [Fact]
        public void Polynomial_PolynomialAddition_ReturnsPolynomialsAdded()
        {
            // Arrange
            var polynomialA = new Polynomial(2, 3, 5, 3);
            var polynomialB = new Polynomial(3, 4, -2);

            // Act
            var result = PolynomialMath.Add(polynomialA, polynomialB);

            // Assert

            // (2 + 3x + 5x^2 + 3x^3) + (3 + 4x - 2x^2) = 5 + 7x + 3x^2 + 3x^3 
            Assert.Equal(3, result.Degree);
            Assert.Equal(4, result.Length);

            Assert.Equal(5, result[0].Coeffecient);
            Assert.Equal(0, result[0].Exponent);

            Assert.Equal(7, result[1].Coeffecient);
            Assert.Equal(1, result[1].Exponent);

            Assert.Equal(3, result[2].Coeffecient);
            Assert.Equal(2, result[2].Exponent);

            Assert.Equal(3, result[3].Coeffecient);
            Assert.Equal(3, result[3].Exponent);
        }

        #endregion

        #region Subtraction

        [Fact]
        public void Polynomial_FractionSubtraction_ReturnsPolynomialsSubtracted()
        {
            // Arrange
            var polynomialA = new Polynomial(2, 3, 5, 3);
            var fraction = new Fraction(2, 1);

            // Act
            var result = PolynomialMath.Subtract(polynomialA, fraction);

            // Assert

            // (2 + 3x + 5x^2 + 3x^3) - (2) = 3x + 5x^2 + 3x^3 
            Assert.Equal(3, result.Degree);
            Assert.Equal(3, result.Length);

            Assert.Equal(3, result[0].Coeffecient);
            Assert.Equal(1, result[0].Exponent);

            Assert.Equal(5, result[1].Coeffecient);
            Assert.Equal(2, result[1].Exponent);

            Assert.Equal(3, result[2].Coeffecient);
            Assert.Equal(3, result[2].Exponent);
        }

        [Fact]
        public void Polynomial_PolynomialTermSubtraction_ReturnsPolynomialsSubtracted()
        {
            // Arrange
            var polynomialA = new Polynomial(2, 3, 5, 3);
            var polynomialTerm = new PolynomialTerm(2, 1, 2);

            // Act
            var result = PolynomialMath.Subtract(polynomialA, polynomialTerm);

            // Assert

            // (2 + 3x + 5x^2 + 3x^3) - (2x^2) = 2 + 3x + 3x^2 + 3x^3 
            Assert.Equal(3, result.Degree);
            Assert.Equal(4, result.Length);

            Assert.Equal(2, result[0].Coeffecient);
            Assert.Equal(0, result[0].Exponent);

            Assert.Equal(3, result[1].Coeffecient);
            Assert.Equal(1, result[1].Exponent);

            Assert.Equal(3, result[2].Coeffecient);
            Assert.Equal(2, result[2].Exponent);

            Assert.Equal(3, result[3].Coeffecient);
            Assert.Equal(3, result[3].Exponent);
        }

        [Fact]
        public void Polynomial_PolynomialSubtraction_ReturnsPolynomialsSubtracted()
        {
            // Arrange
            var polynomialA = new Polynomial(2, 3, 5, 3);
            var polynomialB = new Polynomial(3, 4, -2);

            // Act
            var result = PolynomialMath.Subtract(polynomialA, polynomialB);

            // Assert

            // (2 + 3x + 5x^2 + 3x^3) - (3 + 4x - 2x^2) = -1 - x + 7x^2 + 3x^3 
            Assert.Equal(3, result.Degree);
            Assert.Equal(4, result.Length);

            Assert.Equal(-1, result[0].Coeffecient);
            Assert.Equal(0, result[0].Exponent);

            Assert.Equal(-1, result[1].Coeffecient);
            Assert.Equal(1, result[1].Exponent);

            Assert.Equal(7, result[2].Coeffecient);
            Assert.Equal(2, result[2].Exponent);

            Assert.Equal(3, result[3].Coeffecient);
            Assert.Equal(3, result[3].Exponent);
        }

        #endregion

        #region Mod

        [Fact]
        public void Polynomial_Modulo_ReturnsExpectedPolynomial()
        {
            // Arrange
            var polynomial = new Polynomial(2, 5, 6, 3, 1, 4);

            // Act
            var result = polynomial % 2;

            // Assert

            // 1x + x^3 + x^4

            Assert.Equal(4, result.Degree);
            Assert.Equal(3, result.Length);

            Assert.Equal(1, result[0].Coeffecient);
            Assert.Equal(1, result[0].Exponent);

            Assert.Equal(1, result[1].Coeffecient);
            Assert.Equal(3, result[1].Exponent);

            Assert.Equal(1, result[2].Coeffecient);
            Assert.Equal(4, result[2].Exponent);
        }

        #endregion

        #region Extended Euclidean

        [Fact]
        public static void ExtendedEuclidean_CalculatesExpected()
        {
            // Test sample taken from https://www.youtube.com/watch?v=Y3CQn_KOAkk

            // Arrange
            var fPolynomial = new Polynomial(-2, -3, -2, 0, 1); // -2 - 3x - 2x^2 + x^4
            var gPolynomial = new Polynomial(1, 4, 4, 1); // 1 + 4x + 4x^2 + x^3

            // Act
            var result = PolynomialMath.ExtendedEuclidean(fPolynomial, gPolynomial);

            // Assert
            var expectedGcd = new Polynomial(1, 1); // x + 1
            // -3/11 - 3/11x + 5/22x^2 
            var expectedU = new Polynomial(new PolynomialTerm(-3, 11), new PolynomialTerm(-3, 11, 1), new PolynomialTerm(5, 22, 2));
            // -7/11 - 5/22x
            var expectedV = new Polynomial(new PolynomialTerm(-7, 11), new PolynomialTerm(-5, 22, 1));

            Assert.Equal(expectedGcd, result.Gcd);
            Assert.Equal(expectedU, result.U);
            Assert.Equal(expectedV, result.V);
        }

        #endregion
    }
}