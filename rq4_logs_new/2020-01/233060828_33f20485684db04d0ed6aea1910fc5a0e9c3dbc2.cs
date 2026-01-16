using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Text;
using StatsCalc_IO;

namespace StatsCalc_IOTests
{
    public class MainStatCalculatorTests
    {

        private readonly double[] ArrayList = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        Mean mn= new Mean();
        StandardDeviation sd = new StandardDeviation();
        Mode mod = new Mode();
        Median mdn = new Median();
        Variance rv = new Variance();

        [TestMethod()]
        public void meanTest()
        {
            Assert.AreEqual(01, mn.meanv(ArrayList));
        }

        [TestMethod()]
        public void StandardDeviationvTest()
        {
            Assert.AreEqual(2.9154759474226504, sd.StandardDeviationv(ArrayList));
        }

        [TestMethod()]
        public void modevTest()
        {
            Assert.AreEqual(01, mod.modev(ArrayList));
        }

        [TestMethod()]
        public void medianvTest()
        {
            Assert.AreEqual(6, mdn.medianv(ArrayList));
        }

        [TestMethod()]
        public void variancevTest()
        {
            Assert.AreEqual(8.5, rv.variancev(ArrayList));
        }



    }
}