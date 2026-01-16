using System;
using System.Collections.Generic;
using Xunit;
using StringTable;

namespace StringTable.Tests
{
    public class Table2x2OfPoints
    {
        class Point
        {
            public int X { get; set; }
            public int Y { get; set; }
        }

        List<Point> listOfPoints;

        public Table2x2OfPoints()
        {
            listOfPoints = new List<Point> {
                new Point() { X = 1, Y = 3 },
                new Point() { X = 2, Y = 5 }
            };
        }

        [Fact]
        public void ReturnDefaultStringTable()
        {
            var stringTable = listOfPoints.ToTable().ToString();

            Assert.Equal(
                string.Join(Environment.NewLine,
                    "| X | Y |",
                    "| - | - |",
                    "| 1 | 3 |",
                    "| 2 | 5 |"
                ) + Environment.NewLine,
                stringTable
            );
        }
    }

    public class Table2x2OfAnonymousTypes
    {
        List<dynamic> list;

        public Table2x2OfAnonymousTypes()
        {
            list = new List<dynamic> {
                new { A = 1, B = 3 },
                new { C = 2, D = 5 }
            };
        }

        [Fact]
        public void ReturnDefaultStringTable()
        {
            var stringTable = list.ToTable(derived: true).ToString();

            Assert.Equal(
                string.Join(Environment.NewLine,
                    "| A/C | B/D |",
                    "| --- | --- |",
                    "| 1   | 3   |",
                    "| 2   | 5   |"
                ) + Environment.NewLine,
                stringTable
            );
        }
    }
}