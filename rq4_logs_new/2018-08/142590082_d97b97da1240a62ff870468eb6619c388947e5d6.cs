using Xunit;
using Xunit.Abstractions;
using LeetCodeSolutions;
using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;

namespace LeetCodeTests
{
    public static class Problem149Test
    {
        [Fact]
        public static void SinglePoint()
        {
            //Arrange
            Point[] pointArray = new Point[] {
                new Point(0,0)
            };
            int expected = 1;

            //Act
            int actual = Problem149.MaxPoints(pointArray);

            //Assert
            Assert.Equal(expected, actual);
        }

        [Fact]
        public static void CollinearSmall()
        {
            //Arrange
            Point[] pointArray = new Point[] {
                new Point(0,0),
                new Point(1,1),
                new Point(2,2)
            };
            int expected = 3;

            //Act
            int actual = Problem149.MaxPoints(pointArray);

            //Assert
            Assert.Equal(expected, actual);
        }

        [Fact]
        public static void CollinearLarge()
        {
            //Arrange
            Point[] pointArray = new Point[1000];
            for (int i = 0; i < 1000; i++)
            {
                pointArray[i] = new Point(i, i);
            }
            int expected = 1000;

            //Act
            int actual = Problem149.MaxPoints(pointArray);

            //Assert
            Assert.Equal(expected, actual);
        }

        [Fact]
        public static void RepeatedPoint()
        {
            //Arrange
            Point[] pointArray = new Point[100];
            for (int i = 0; i < 100; i++)
            {
                pointArray[i] = new Point(10, 10);
            }
            int expected = 100;

            //Act
            int actual = Problem149.MaxPoints(pointArray);

            //Assert
            Assert.Equal(expected, actual);
        }

        [Fact]
        public static void TwoCollinear()
        {
            //Arrange
            Point[] pointArray = new Point[100];
            for (int i = 0; i < 100; i++)
            {
                pointArray[i] = new Point(i, i*i);
            }
            int expected = 2;

            //Act
            int actual = Problem149.MaxPoints(pointArray);

            //Assert
            Assert.Equal(expected, actual);
        }

        [Fact]
        public static void GridPoints()
        {
            //Arrange
            Point[] pointArray = new Point[900];
            for (int i = 0; i < 30; i++)
            {
                for (int j = 0; j < 30; j++)
                {
                    pointArray[j + (30 * i)] = new Point(i, j);
                }
            }
            int expected = 30;

            //Act
            int actual = Problem149.MaxPoints(pointArray);

            //Assert
            Assert.Equal(expected, actual);
        }
    }
}