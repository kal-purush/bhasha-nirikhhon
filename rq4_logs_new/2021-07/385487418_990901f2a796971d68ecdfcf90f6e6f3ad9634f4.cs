using NUnit.Framework;
using System;


namespace CalculateArea.Test
{
    
    
    public class Tests
    {
        private string _expectedMessage;
        private double _expectedResult;

        [SetUp]
        public void Setup()
        {
        }

        [Test]
        public void AreaOfCircle_NormalArea()
        {
            _expectedResult = 78.539;
                double result =Geometry.areaOfCircle(5);
            Assert.AreEqual(_expectedResult, result, 0.001, "78.54 is an area of Circle of radius 5");
        }

        [Test]
        public void AreaOfCircle_NegativeRadiusArea()
        {       
            try
            {
                double result = Geometry.areaOfCircle(-5);
                Assert.Fail("no exception thrown");
            }
            catch (Exception ex)
            {
                Assert.That(ex.Message, Is.EqualTo("Error! Enter positive arguments!"));
            }
        }

        [Test]
        public void AreaOfRectangle_NormalArea()
        {
            _expectedResult = 25;
            double result = Geometry.areaOfRectangle(5,5);
            Assert.AreEqual(_expectedResult, result, 0.001, "25 is an area of Rectangle of 5 x 5");
        }

        [Test]
        public void AreaOfRectangle_NegativeArgumentArea()
        {
            try
            {
                double result = Geometry.areaOfRectangle(-5,5);
                Assert.Fail("no exception thrown");
            }
            catch (Exception ex)
            {
                Assert.That(ex.Message, Is.EqualTo("Error! Enter positive arguments!"));
            }
        }

        [Test]
        public void AreaOfTriangle_NormalArea()
        {
            _expectedResult = 12.5;
            double result = Geometry.areaOfTriangle(5, 5);
            Assert.AreEqual(_expectedResult, result, 0.001, "12.5 is an area of Rectangle of h5 x b5");
        }

        [Test]
        public void AreaOfTriangle_NegativeArgumentArea()
        {
            try
            {
                double result = Geometry.areaOfTriangle(-5, 5);
                Assert.Fail("no exception thrown");
            }
            catch (Exception ex)
            {
                Assert.That(ex.Message, Is.EqualTo("Error! Enter positive arguments!"));
            }
        }
    }
}