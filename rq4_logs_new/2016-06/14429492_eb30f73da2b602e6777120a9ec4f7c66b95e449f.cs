using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using NUnit.Framework;

namespace KlotosLib.UnitTests
{
    [NUnit.Framework.TestFixture]
    public class GeometricAngleTest
    {
        [Test]
        public void InstancePropertiesAndMethods()
        {
            GeometricAngle a1 = GeometricAngle.FromDegrees(270);
            GeometricAngle a2 = GeometricAngle.FromQuadrants(3);
            Assert.IsTrue(a1 == a2);
            Assert.IsTrue(a1.OriginalUnit == GeometricAngle.MeasurementUnit.Degree);
            Assert.IsTrue(a2.OriginalUnit == GeometricAngle.MeasurementUnit.Quadrant);

            Assert.AreEqual(a1.InTurns, a2.InTurns);
            Assert.AreEqual(a1.InRadians, a2.InRadians);
            Assert.AreEqual(a1.InDegrees, a2.InDegrees);
            Assert.AreEqual(a1.InQuadrants, a2.InQuadrants);
            Assert.AreEqual(a1.InSextants, a2.InSextants);
            Assert.AreEqual(a1.InBinaryDegrees, a2.InBinaryDegrees);
            Assert.AreEqual(a1.InGrads, a2.InGrads);
            Assert.AreEqual(a1.Type, a2.Type);
            Assert.IsTrue(a1.Type == GeometricAngle.AngleType.Reflex);

            Assert.AreEqual(3.0, a1.GetValueInUnitType(GeometricAngle.MeasurementUnit.Quadrant));
            Assert.AreEqual(192, a1.GetValueInUnitType(GeometricAngle.MeasurementUnit.BinaryDegree));
            Assert.Throws<InvalidEnumArgumentException>(delegate
            {
                Double res = a1.GetValueInUnitType((GeometricAngle.MeasurementUnit)7);
            });
        }

        [Test]
        public void GetOneTurnValueForUnit()
        {
            Assert.AreEqual(1.0, GeometricAngle.GetOneTurnValueForUnit(GeometricAngle.MeasurementUnit.Turn));
            Assert.AreEqual(Math.PI * 2, GeometricAngle.GetOneTurnValueForUnit(GeometricAngle.MeasurementUnit.Radian));
            Assert.AreEqual(360, GeometricAngle.GetOneTurnValueForUnit(GeometricAngle.MeasurementUnit.Degree));
            Assert.AreEqual(256, GeometricAngle.GetOneTurnValueForUnit(GeometricAngle.MeasurementUnit.BinaryDegree));
            Assert.AreEqual(4, GeometricAngle.GetOneTurnValueForUnit(GeometricAngle.MeasurementUnit.Quadrant));
            Assert.AreEqual(6, GeometricAngle.GetOneTurnValueForUnit(GeometricAngle.MeasurementUnit.Sextant));
            Assert.AreEqual(400, GeometricAngle.GetOneTurnValueForUnit(GeometricAngle.MeasurementUnit.Grad));
            Assert.Throws<InvalidEnumArgumentException>(delegate
            {
                Double res = GeometricAngle.GetOneTurnValueForUnit((GeometricAngle.MeasurementUnit)7);
            });
        }

        [Test]
        public void GetAngleValueForTypeAndUnit()
        {
            Assert.AreEqual(0.5, GeometricAngle.GetAngleValueForTypeAndUnit(GeometricAngle.AngleType.Straight, GeometricAngle.MeasurementUnit.Turn));
            Assert.AreEqual(Math.PI * 2.0, GeometricAngle.GetAngleValueForTypeAndUnit(GeometricAngle.AngleType.Full, GeometricAngle.MeasurementUnit.Radian));
            Assert.AreEqual(90, GeometricAngle.GetAngleValueForTypeAndUnit(GeometricAngle.AngleType.Right, GeometricAngle.MeasurementUnit.Degree));
            Assert.AreEqual(0.0, GeometricAngle.GetAngleValueForTypeAndUnit(GeometricAngle.AngleType.Zero, GeometricAngle.MeasurementUnit.Quadrant));
            Assert.Throws<InvalidOperationException>(delegate
            {
                Double res = GeometricAngle.GetAngleValueForTypeAndUnit(GeometricAngle.AngleType.Reflex, GeometricAngle.MeasurementUnit.Quadrant);
            });
            Assert.Throws<InvalidEnumArgumentException>(delegate
            {
                Double res = GeometricAngle.GetAngleValueForTypeAndUnit((GeometricAngle.AngleType)7, GeometricAngle.MeasurementUnit.Quadrant);
            });
        }

        [Test]
        public void GetAngleTypeFromValueOfUnit()
        {
            Assert.AreEqual(GeometricAngle.AngleType.Zero, GeometricAngle.GetAngleTypeFromValueOfUnit(0.0, GeometricAngle.MeasurementUnit.Radian));
            Assert.AreEqual(GeometricAngle.AngleType.Acute, GeometricAngle.GetAngleTypeFromValueOfUnit(16, GeometricAngle.MeasurementUnit.BinaryDegree));
            Assert.AreEqual(GeometricAngle.AngleType.Right, GeometricAngle.GetAngleTypeFromValueOfUnit(64, GeometricAngle.MeasurementUnit.BinaryDegree));
            Assert.AreEqual(GeometricAngle.AngleType.Obtuse, GeometricAngle.GetAngleTypeFromValueOfUnit(199, GeometricAngle.MeasurementUnit.Grad));
            Assert.AreEqual(GeometricAngle.AngleType.Straight, GeometricAngle.GetAngleTypeFromValueOfUnit(Math.PI, GeometricAngle.MeasurementUnit.Radian));
            Assert.AreEqual(GeometricAngle.AngleType.Reflex, GeometricAngle.GetAngleTypeFromValueOfUnit(4, GeometricAngle.MeasurementUnit.Sextant));
            Assert.AreEqual(GeometricAngle.AngleType.Full, GeometricAngle.GetAngleTypeFromValueOfUnit(1, GeometricAngle.MeasurementUnit.Turn));
            Assert.Throws<ArgumentOutOfRangeException>(delegate
            {
                GeometricAngle.AngleType type = GeometricAngle.GetAngleTypeFromValueOfUnit(-1, GeometricAngle.MeasurementUnit.Degree);
            });
            Assert.Throws<ArgumentOutOfRangeException>(delegate
            {
                GeometricAngle.AngleType type = GeometricAngle.GetAngleTypeFromValueOfUnit(361, GeometricAngle.MeasurementUnit.Degree);
            });
        }

        [TestCase(90, GeometricAngle.MeasurementUnit.Degree, 90, GeometricAngle.MeasurementUnit.Degree, ExpectedResult = true)]
        [TestCase(90, GeometricAngle.MeasurementUnit.Degree, 100, GeometricAngle.MeasurementUnit.Grad, ExpectedResult = true)]
        [TestCase(90, GeometricAngle.MeasurementUnit.Degree, 200, GeometricAngle.MeasurementUnit.Grad, ExpectedResult = false)]
        [TestCase(Math.PI / 2.0, GeometricAngle.MeasurementUnit.Radian, 100, GeometricAngle.MeasurementUnit.Grad, ExpectedResult = true)]
        [TestCase(100, GeometricAngle.MeasurementUnit.Grad, Math.PI / 2.0, GeometricAngle.MeasurementUnit.Radian, ExpectedResult = true)]
        [TestCase(3, GeometricAngle.MeasurementUnit.Sextant, 2, GeometricAngle.MeasurementUnit.Quadrant, ExpectedResult = true)]
        [TestCase(3, GeometricAngle.MeasurementUnit.Sextant, 3, GeometricAngle.MeasurementUnit.Quadrant, ExpectedResult = false)]
        [TestCase(Math.PI, GeometricAngle.MeasurementUnit.Radian, 1, GeometricAngle.MeasurementUnit.Turn, ExpectedResult = false)]
        [TestCase(Math.PI, GeometricAngle.MeasurementUnit.Radian, 0.5, GeometricAngle.MeasurementUnit.Turn, ExpectedResult = true)]
        [TestCase(Math.PI * 2.0, GeometricAngle.MeasurementUnit.Radian, 1, GeometricAngle.MeasurementUnit.Turn, ExpectedResult = true)]
        [TestCase(270, GeometricAngle.MeasurementUnit.Degree, 3, GeometricAngle.MeasurementUnit.Quadrant, ExpectedResult = true)]
        [TestCase(270, GeometricAngle.MeasurementUnit.Degree, 3, GeometricAngle.MeasurementUnit.Sextant, ExpectedResult = false)]
        [TestCase(180, GeometricAngle.MeasurementUnit.Degree, 540, GeometricAngle.MeasurementUnit.Degree, ExpectedResult = true)]
        [TestCase(360, GeometricAngle.MeasurementUnit.Degree, 720, GeometricAngle.MeasurementUnit.Degree, ExpectedResult = true)]
        [TestCase(0, GeometricAngle.MeasurementUnit.Degree, 0, GeometricAngle.MeasurementUnit.Degree, ExpectedResult = true)]
        [TestCase(0, GeometricAngle.MeasurementUnit.Degree, -360, GeometricAngle.MeasurementUnit.Degree, ExpectedResult = false)]
        [TestCase(360, GeometricAngle.MeasurementUnit.Degree, -360, GeometricAngle.MeasurementUnit.Degree, ExpectedResult = true)]
        [TestCase(-450, GeometricAngle.MeasurementUnit.Degree, 270, GeometricAngle.MeasurementUnit.Degree, ExpectedResult = true)]
        [TestCase(270, GeometricAngle.MeasurementUnit.Degree, 3, (GeometricAngle.MeasurementUnit)7, ExpectedException = typeof(InvalidEnumArgumentException))]
        public Boolean Equality(
            Double angle1Value, GeometricAngle.MeasurementUnit angle1MeasurementUnit, 
            Double angle2Value, GeometricAngle.MeasurementUnit angle2MeasurementUnit
            )
        {
            GeometricAngle first = GeometricAngle.FromSpecifiedUnit(angle1Value, angle1MeasurementUnit);
            GeometricAngle second = GeometricAngle.FromSpecifiedUnit(angle2Value, angle2MeasurementUnit);
            return GeometricAngle.AreEqual(first, second);
        }

        [Test]
        public void Equality()
        {
            GeometricAngle a1 = GeometricAngle.FromRadians(Math.PI);
            Assert.AreEqual(a1.Type, GeometricAngle.AngleType.Straight);
            GeometricAngle a2 = GeometricAngle.FromBinaryDegrees(128);
            Assert.AreEqual(a1, a2);
            GeometricAngle a3 = GeometricAngle.FromSextants(3);
            GeometricAngle a4 = GeometricAngle.FromTurns(0.5);
            GeometricAngle a5 = GeometricAngle.FromRadians(Math.PI/2.0);
            Assert.IsTrue(a3 == a4);
            Assert.IsTrue(GeometricAngle.AreEqual(a1, a2, a3));
            Assert.IsTrue(GeometricAngle.AreEqual(new GeometricAngle[2] { a2, a3 }));
            Assert.IsTrue(GeometricAngle.AreEqual(a1, a2, a3, a4));
            Assert.IsFalse(GeometricAngle.AreEqual(a1, a2, a3, a4, a5));
            Assert.Throws<ArgumentNullException>(delegate { Boolean res = GeometricAngle.AreEqual(null); });
            Assert.Throws<ArgumentException>(delegate { Boolean res = GeometricAngle.AreEqual(new GeometricAngle[0]{}); });
            Assert.Throws<ArgumentException>(delegate { Boolean res = GeometricAngle.AreEqual(a1); });

            Assert.IsFalse(a1.Equals(null));
            Assert.IsFalse(a1.Equals(new Object()));
        }

        [Test]
        public void GetHashCodeTest()
        {
            GeometricAngle a1 = GeometricAngle.FromRadians(Math.PI);
            Int32 expectedHashCode = Math.PI.GetHashCode();
            expectedHashCode = (expectedHashCode * 397) ^ GeometricAngle.MeasurementUnit.Radian.GetHashCode();
            Assert.AreEqual(expectedHashCode, a1.GetHashCode());
        }

        [TestCase(90, GeometricAngle.MeasurementUnit.Degree, 90, GeometricAngle.MeasurementUnit.Degree, ExpectedResult = 0)]
        [TestCase(90, GeometricAngle.MeasurementUnit.Degree, 180, GeometricAngle.MeasurementUnit.Degree, ExpectedResult = -1)]
        [TestCase(180, GeometricAngle.MeasurementUnit.Degree, 90, GeometricAngle.MeasurementUnit.Degree, ExpectedResult = 1)]
        [TestCase(Math.PI, GeometricAngle.MeasurementUnit.Radian, 199, GeometricAngle.MeasurementUnit.Grad, ExpectedResult = 1)]
        [TestCase(399, GeometricAngle.MeasurementUnit.Grad, Math.PI * 2.0, GeometricAngle.MeasurementUnit.Radian, ExpectedResult = -1)]
        [TestCase(0, GeometricAngle.MeasurementUnit.Degree, 1, GeometricAngle.MeasurementUnit.Turn, ExpectedResult = -1)]
        [TestCase(0, GeometricAngle.MeasurementUnit.Degree, 0, GeometricAngle.MeasurementUnit.Turn, ExpectedResult = 0)]
        public Int32 Comparison(
            Double angle1Value, GeometricAngle.MeasurementUnit angle1MeasurementUnit,
            Double angle2Value, GeometricAngle.MeasurementUnit angle2MeasurementUnit
            )
        {
            GeometricAngle first = GeometricAngle.FromSpecifiedUnit(angle1Value, angle1MeasurementUnit);
            GeometricAngle second = GeometricAngle.FromSpecifiedUnit(angle2Value, angle2MeasurementUnit);
            Int32 firstComparison = first.CompareTo((Object)second);
            Int32 secondComparison = first.CompareTo(second);
            Int32 thirdComparison = GeometricAngle.Compare(first, second);
            Assert.IsTrue(firstComparison == secondComparison && secondComparison == thirdComparison);
            return firstComparison;
        }

        [Test]
        public void Comparison()
        {
            GeometricAngle a1 = GeometricAngle.FromBinaryDegrees(256);
            Assert.Throws<ArgumentNullException>(delegate { Int32 res1 = a1.CompareTo(null); });
            Assert.Throws<InvalidOperationException>(delegate { Int32 res2 = a1.CompareTo("abcd"); });
        }

        [TestCase(90, GeometricAngle.MeasurementUnit.Degree, 90, GeometricAngle.MeasurementUnit.Degree, 1, ExpectedResult = true)]
        [TestCase(90, GeometricAngle.MeasurementUnit.Degree, 128, GeometricAngle.MeasurementUnit.BinaryDegree, 1, ExpectedResult = false)]
        [TestCase(90, GeometricAngle.MeasurementUnit.Degree, 128, GeometricAngle.MeasurementUnit.BinaryDegree, 2, ExpectedResult = true)]
        [TestCase(3, GeometricAngle.MeasurementUnit.Sextant, 128, GeometricAngle.MeasurementUnit.BinaryDegree, 1, ExpectedResult = true)]
        [TestCase(3, GeometricAngle.MeasurementUnit.Sextant, 128, GeometricAngle.MeasurementUnit.BinaryDegree, 2, ExpectedResult = false)]
        [TestCase(3, GeometricAngle.MeasurementUnit.Sextant, 128, GeometricAngle.MeasurementUnit.BinaryDegree, 5, ExpectedResult = true)]
        [TestCase(3, GeometricAngle.MeasurementUnit.Sextant, 128, GeometricAngle.MeasurementUnit.BinaryDegree, 6, ExpectedResult = true)]
        [TestCase(3, GeometricAngle.MeasurementUnit.Sextant, 128, GeometricAngle.MeasurementUnit.BinaryDegree, 3, ExpectedResult = false)]
        [TestCase(3, GeometricAngle.MeasurementUnit.Sextant, 128, GeometricAngle.MeasurementUnit.BinaryDegree, 4, ExpectedResult = false)]
        [TestCase(3, GeometricAngle.MeasurementUnit.Quadrant, Math.PI, GeometricAngle.MeasurementUnit.Turn, 1, ExpectedResult = false)]
        [TestCase(3, GeometricAngle.MeasurementUnit.Quadrant, Math.PI, GeometricAngle.MeasurementUnit.Turn, 2, ExpectedResult = true)]
        [TestCase(3, GeometricAngle.MeasurementUnit.Quadrant, Math.PI, GeometricAngle.MeasurementUnit.Turn, 3, ExpectedResult = true)]
        [TestCase(3, GeometricAngle.MeasurementUnit.Quadrant, Math.PI, GeometricAngle.MeasurementUnit.Turn, 4, ExpectedResult = false)]
        [TestCase(3, GeometricAngle.MeasurementUnit.Quadrant, Math.PI, GeometricAngle.MeasurementUnit.Turn, 5, ExpectedResult = true)]
        [TestCase(3, GeometricAngle.MeasurementUnit.Quadrant, Math.PI, GeometricAngle.MeasurementUnit.Turn, 6, ExpectedResult = false)]
        [TestCase(Math.PI, GeometricAngle.MeasurementUnit.Turn, 3, GeometricAngle.MeasurementUnit.Quadrant, 1, ExpectedResult = false)]
        [TestCase(Math.PI, GeometricAngle.MeasurementUnit.Turn, 3, GeometricAngle.MeasurementUnit.Quadrant, 2, ExpectedResult = true)]
        [TestCase(Math.PI, GeometricAngle.MeasurementUnit.Turn, 3, GeometricAngle.MeasurementUnit.Quadrant, 3, ExpectedResult = false)]
        [TestCase(Math.PI, GeometricAngle.MeasurementUnit.Turn, 3, GeometricAngle.MeasurementUnit.Quadrant, 4, ExpectedResult = true)]
        [TestCase(Math.PI, GeometricAngle.MeasurementUnit.Turn, 3, GeometricAngle.MeasurementUnit.Quadrant, 5, ExpectedResult = false)]
        [TestCase(Math.PI, GeometricAngle.MeasurementUnit.Turn, 3, GeometricAngle.MeasurementUnit.Quadrant, 6, ExpectedResult = true)]
        [TestCase(Math.PI, GeometricAngle.MeasurementUnit.Turn, 3, GeometricAngle.MeasurementUnit.Quadrant, 7, ExpectedException = typeof(InvalidOperationException))]
        public Boolean EqualityGreaterLesserOperators(
            Double angle1Value, GeometricAngle.MeasurementUnit angle1MeasurementUnit,
            Double angle2Value, GeometricAngle.MeasurementUnit angle2MeasurementUnit,
            Byte operatorType)//operatorType: 1 - ==; 2 - !=; 3 - >; 4 - <; 5 - >=; 6 - <=
        {
            GeometricAngle first = GeometricAngle.FromSpecifiedUnit(angle1Value, angle1MeasurementUnit);
            GeometricAngle second = GeometricAngle.FromSpecifiedUnit(angle2Value, angle2MeasurementUnit);
            switch (operatorType)
            {
                case 1:
                    return first == second;
                case 2:
                    return first != second;
                case 3:
                    return first > second;
                case 4:
                    return first < second;
                case 5:
                    return first >= second;
                case 6:
                    return first <= second;
                default:
                    throw new InvalidOperationException();
            }
        }

        [TestCase(90, GeometricAngle.MeasurementUnit.Degree, 90, GeometricAngle.MeasurementUnit.Degree, GeometricAngle.MeasurementUnit.Radian, ExpectedResult = Math.PI)]
        [TestCase(180, GeometricAngle.MeasurementUnit.Degree, 180, GeometricAngle.MeasurementUnit.Degree, GeometricAngle.MeasurementUnit.Degree, ExpectedResult = 360)]
        [TestCase(180, GeometricAngle.MeasurementUnit.Degree, 180, GeometricAngle.MeasurementUnit.Degree, GeometricAngle.MeasurementUnit.Radian, ExpectedResult = Math.PI * 2.0)]
        [TestCase(180, GeometricAngle.MeasurementUnit.Degree, 270, GeometricAngle.MeasurementUnit.Degree, GeometricAngle.MeasurementUnit.Degree, ExpectedResult = 90)]
        [TestCase(270, GeometricAngle.MeasurementUnit.Degree, 180, GeometricAngle.MeasurementUnit.Degree, GeometricAngle.MeasurementUnit.Degree, ExpectedResult = 90)]
        [TestCase(270, GeometricAngle.MeasurementUnit.Degree, 270, GeometricAngle.MeasurementUnit.Degree, GeometricAngle.MeasurementUnit.Radian, ExpectedResult = Math.PI)]
        [TestCase(270, GeometricAngle.MeasurementUnit.Degree, 192, GeometricAngle.MeasurementUnit.BinaryDegree, GeometricAngle.MeasurementUnit.Radian, ExpectedResult = Math.PI)]
        [TestCase(256, GeometricAngle.MeasurementUnit.BinaryDegree, 400, GeometricAngle.MeasurementUnit.Grad, GeometricAngle.MeasurementUnit.Sextant, ExpectedResult = 6)]
        public Double Addition(
            Double angle1Value, GeometricAngle.MeasurementUnit angle1MeasurementUnit,
            Double angle2Value, GeometricAngle.MeasurementUnit angle2MeasurementUnit,
            GeometricAngle.MeasurementUnit outputAngleMeasurementUnit)
        {
            GeometricAngle first = GeometricAngle.FromSpecifiedUnit(angle1Value, angle1MeasurementUnit);
            GeometricAngle second = GeometricAngle.FromSpecifiedUnit(angle2Value, angle2MeasurementUnit);
            GeometricAngle sum = first + second;
            return sum.GetValueInUnitType(outputAngleMeasurementUnit);
        }

        [TestCase(90, GeometricAngle.MeasurementUnit.Degree, 90, GeometricAngle.MeasurementUnit.Degree, GeometricAngle.MeasurementUnit.Radian, ExpectedResult = 0.0)]
        [TestCase(90, GeometricAngle.MeasurementUnit.Degree, Math.PI, GeometricAngle.MeasurementUnit.Radian, GeometricAngle.MeasurementUnit.Quadrant, ExpectedResult = 3)]
        [TestCase(Math.PI, GeometricAngle.MeasurementUnit.Radian, 3, GeometricAngle.MeasurementUnit.Quadrant, GeometricAngle.MeasurementUnit.Grad, ExpectedResult = 300)]
        [TestCase(64, GeometricAngle.MeasurementUnit.BinaryDegree, 192, GeometricAngle.MeasurementUnit.BinaryDegree, GeometricAngle.MeasurementUnit.Sextant, ExpectedResult = 3)]
        public Double Subtraction(
            Double angle1Value, GeometricAngle.MeasurementUnit angle1MeasurementUnit,
            Double angle2Value, GeometricAngle.MeasurementUnit angle2MeasurementUnit,
            GeometricAngle.MeasurementUnit outputAngleMeasurementUnit)
        {
            GeometricAngle minuend = GeometricAngle.FromSpecifiedUnit(angle1Value, angle1MeasurementUnit);
            GeometricAngle subtrahend = GeometricAngle.FromSpecifiedUnit(angle2Value, angle2MeasurementUnit);
            GeometricAngle difference = minuend - subtrahend;
            return difference.GetValueInUnitType(outputAngleMeasurementUnit);
        }

        [TestCase(90, GeometricAngle.MeasurementUnit.Degree, ExpectedResult = "90 Degrees")]
        [TestCase(300, GeometricAngle.MeasurementUnit.Grad, ExpectedResult = "300 Grads")]
        [TestCase(0.1, GeometricAngle.MeasurementUnit.Turn, ExpectedResult = "0.1 Turns")]
        public String ToString(Double angleValue, GeometricAngle.MeasurementUnit angleMeasurementUnit)
        {
            GeometricAngle geometricAngle = GeometricAngle.FromSpecifiedUnit(angleValue, angleMeasurementUnit);
            return geometricAngle.ToString();
        }

        [Test]
        public void Clone()
        {
            GeometricAngle first = GeometricAngle.FromDegrees(90);
            GeometricAngle second = first.Clone();
            Assert.AreEqual(first, second);
            first = GeometricAngle.FromGrads(300);
            Assert.AreNotEqual(first, second);
            Object third = ((ICloneable) second).Clone();
            Assert.IsInstanceOf<GeometricAngle>(third);
            Assert.IsTrue(second.Equals(third));
        }
    }
}