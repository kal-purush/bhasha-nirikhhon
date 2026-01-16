using System;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Mathmagician.Tests
{
    [TestClass]
    public class OddTests
    {
        [TestMethod]
        public void Odd_EnsureICanCreateAnInstance()
        {
            //arrange (nothing in this case)

            //act (nothing in this case)

            //assert (checks to see that Odd class isn't null)
            Assert.IsNotNull(new Odd());
        }

        [TestMethod]
        public void Odd_IsAListReturned()
        {
            //arrange (nothing in this case)

            //act (nothing in this case)

            //assert (makes a new instance of Odd, runs the CountOddNumbers method with "4" as the input and looks to see 
            //        if a List<int> is returned)
            Assert.IsInstanceOfType(new Odd().CountOddNumbers(4), typeof(List<int>));
        }

        [TestMethod]
        public void Odd_IsTheFifteenthNumberCorrect()
        {
            //arrange (nothing in this case)

            //act (nothing in this case)

            //assert (makes a new instance of Odd, runs the CountOddNumbers method with "15" as the input and looks at
            //        compares element 14 to what the 14th odd number should be)
            Assert.AreEqual(29, new Odd().CountOddNumbers(15)[14]);
        }

        [TestMethod]
        public void Odd_IsTheFiveThousanthNumberCorrect()
        {
            //arrange (nothing in this case)

            //act (nothing in this case)

            //assert (makes a new instance of Odd, runs the CountOddNumbers method with "10000" as the input and looks at
            //        compares element 14 to what the 14th odd number should be)
            Assert.AreEqual(9999, new Odd().CountOddNumbers(10000)[4999]);
        }
    }
}