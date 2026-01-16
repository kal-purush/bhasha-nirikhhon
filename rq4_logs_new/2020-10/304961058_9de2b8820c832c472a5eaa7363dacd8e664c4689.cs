using Microsoft.VisualStudio.TestTools.UnitTesting;
using JoliBateau;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JoliBateau.Tests
{
    [TestClass()]
    public class PointTests
    {
        [TestMethod()]
        public void GetListSuccTest()
        {            
            bool PavageCarre(Point point)
            {
                Point.Pavage = 0;
                Point.DistNoeuds = 1;

                List<GenericNode> succPavageCarre = point.GetListSucc();
                List<GenericNode> testPavageCarre = new List<GenericNode>();
                testPavageCarre.Add(new Point(99, 99));
                testPavageCarre.Add(new Point(99, 100));
                testPavageCarre.Add(new Point(99, 101));
                testPavageCarre.Add(new Point(100, 99));
                testPavageCarre.Add(new Point(100, 101));
                testPavageCarre.Add(new Point(101, 99));
                testPavageCarre.Add(new Point(101, 100));
                testPavageCarre.Add(new Point(101, 101));

                succPavageCarre.Cast<Point>();
                testPavageCarre.Cast<Point>();
                bool test0 = true;
                for (int i = 0; i < succPavageCarre.Count; i++)
                {
                    if (!succPavageCarre[i].IsEqual(testPavageCarre[i]))
                    {
                        test0 = false;
                    }
                }

                return test0;
            }

            bool PavageCroix(Point point)
            {
                // test pavage croix
                Point.Pavage = 1;
                Point.DistNoeuds = 1;

                List<GenericNode> succPavageCroix = point.GetListSucc();
                List<GenericNode> testPavageCroix = new List<GenericNode>();
                testPavageCroix.Add(new Point(99, 100));
                testPavageCroix.Add(new Point(100, 99));
                testPavageCroix.Add(new Point(100, 101));
                testPavageCroix.Add(new Point(101, 100));

                succPavageCroix.Cast<Point>();
                testPavageCroix.Cast<Point>();
                bool test1 = true;
                for (int i = 0; i < testPavageCroix.Count; i++)
                {
                    if (!succPavageCroix[i].IsEqual(testPavageCroix[i]))
                    {
                        test1 = false;
                    }
                }
                return test1;
            }

            Point p = new Point(100, 100);
            bool cond0 = PavageCarre(p);
            bool cond1 = PavageCroix(p);


            Assert.IsTrue(cond0 && cond1);
            
        }

    }
}