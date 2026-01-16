using System;
using System.Linq;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Linq.Expressions;
using ExpressionKit.Unwrap;

namespace WaxUnitTest
{
    [TestClass]
    public class BasicTest
    {
        static Expression<Func<int, int>> Square = x => x * x;
        static Expression<Func<int, int>> SquSquare = Wax.Unwrap<int, int>(
        x => Square.Expand(Square.Expand(x)));
        static Expression<Func<int, int>> Cube = Wax.Unwrap<int, int>(
            x => SquSquare.Expand(x) / x);



        [TestMethod]
        public void SquSquareTest()
        {
            Expression<Func<int,int>> myexp = x=>  (x*x)*(x*x);
            Assert.AreEqual(SquSquare.Body.ToString(), myexp.Body.ToString());
        }

        [TestMethod]
        public void CubeTest()
        {
            Expression<Func<int, int>> myexp = x => (x * x) * (x * x) / x;
            Assert.AreEqual(Cube.Body.ToString(), myexp.Body.ToString());
        }

        [TestMethod]
        public void AndTest()
        {
            Expression<Func<bool, bool, bool>> and = Wax.Unwrap<bool, bool, bool>((x, y) => x && y);
            Func<bool, bool, bool> and_func = and.Compile();
            Assert.AreEqual(and_func(true, true), true);
            Assert.AreEqual(and_func(true, false), false);
            Assert.AreEqual(and_func(false, true), false);
            Assert.AreEqual(and_func(false, false), false);
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void InvalidOperationExceptionTest()
        {
            Console.WriteLine(Wax.Expand<int, int>(Square, 10));
        }

        [TestMethod]
        public void SelectTest()
        {
            Expression<Func<int, int>> AddTwo = x => x + 2;

            int[] array = new int[] { 1, 2, 3, 4 };
            var queryable = array.AsQueryable();
            var s = Wax.UnwrappedSelect<int, int>(queryable, AddTwo);
            Assert.AreEqual(s.Sum(), 18);
        }

        [TestMethod]
        public void WhereTest()
        {
            Expression<Func<int, bool>> SelectFour = x => x == 4;
            Expression<Func<int, int, bool>> SelectNumber = (x, y) => x == y;

            int[] array = new int[] { 1, 2, 3, 4, 5, 6, 5, 4 };
            var queryable = array.AsQueryable();

            var t = Wax.UnwrappedWhere<int>(queryable, SelectFour);
            Assert.AreEqual(t.Count(), 2);
        }


    }

}

