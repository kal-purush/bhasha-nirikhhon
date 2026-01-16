using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Linq.Expressions;
using ExpressionKit.Unwrap;

namespace WaxUnitTest
{
    [TestClass]
    public class OneParameterExpressionTest
    {

        static Expression<Func<bool, bool>> false_expr = Wax.Unwrap<bool, bool>(x => Wax.Constant<bool>(false));
        static Expression<Func<bool, bool>> true_expr = Wax.Inverse<bool>(Wax.False<bool>());

        [TestMethod]
        public void NotTest()
        {
            Expression<Func<bool, bool>> not = Wax.Unwrap<bool, bool>(x => !x);
            var not_f = not.Compile();
            Assert.AreEqual(not_f(true), false);
            Assert.AreEqual(not_f(false), true);
        }

        [TestMethod]
        public void FalseExpressionTest()
        {
            var false_expr_f = false_expr.Compile();
            var false_expr_f2 = false_expr.And(true_expr).Compile();
            Assert.AreEqual(false_expr_f(false), false);
            Assert.AreEqual(false_expr_f(true), false);
            Assert.AreEqual(false_expr_f2(false), false);
            Assert.AreEqual(false_expr_f2(true), false);
        }

        [TestMethod]
        public void TrueExpressionTest()
        {
            var true_expr_f = true_expr.Compile();
            var true_expr_f2 = true_expr.Or(false_expr).Compile();
            Assert.AreEqual(true_expr_f(false), true);
            Assert.AreEqual(true_expr_f(true), true);
            Assert.AreEqual(true_expr_f2(false), true);
            Assert.AreEqual(true_expr_f2(true), true);

        }


    }
}