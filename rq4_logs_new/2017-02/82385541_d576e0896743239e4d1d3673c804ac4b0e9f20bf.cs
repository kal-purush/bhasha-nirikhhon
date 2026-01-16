namespace RskSharp.Tests.Vm
{
    using System;
    using System.Numerics;
    using RskSharp.Vm;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class DataWordTests
    {
        [TestMethod]
        public void CreateDataWordUsingPositiveInteger()
        {
            var dw = new DataWord(1);

            var result = dw.Bytes;

            Assert.IsNotNull(result);
            Assert.AreEqual(32, result.Length);

            for (int k = 0; k < 31; k++)
                Assert.AreEqual(0x00, result[k]);

            Assert.AreEqual(0x01, result[31]);
        }

        [TestMethod]
        public void CreateDataWordUsingBytes()
        {
            var dw = new DataWord(new byte[] { 0x01, 0x02 });

            Assert.AreEqual(new BigInteger(258), dw.Value);

            var result = dw.Bytes;

            for (int k = 0; k < 30; k++)
                Assert.AreEqual(0x00, result[k]);

            Assert.AreEqual(0x01, result[30]);
            Assert.AreEqual(0x02, result[31]);
        }

        [TestMethod]
        public void CreateDataWordUsingNegativeInteger()
        {
            var dw = new DataWord(-1);

            var result = dw.Bytes;

            Assert.IsNotNull(result);
            Assert.AreEqual(32, result.Length);

            for (int k = 0; k < 32; k++)
                Assert.AreEqual(0xff, result[k]);
        }

        [TestMethod]
        public void Equals()
        {
            var dw1 = new DataWord(1);
            var dw2 = new DataWord(2);
            var dw3 = new DataWord(1);

            Assert.AreEqual(dw1, dw1);
            Assert.AreEqual(dw1, dw3);
            Assert.AreEqual(dw3, dw1);

            Assert.IsFalse(dw1.Equals(null));

            Assert.AreNotEqual(dw1, null);
            Assert.AreNotEqual(dw1, "foo");
            Assert.AreNotEqual(dw1, 42);
            Assert.AreNotEqual(dw1, dw2);

            Assert.AreEqual(dw1.GetHashCode(), dw3.GetHashCode());
        }

        [TestMethod]
        public void NegatePositiveInteger()
        {
            var dw = new DataWord(3);

            var result = dw.Negate();

            Assert.IsNotNull(result);
            Assert.AreEqual(new DataWord(-3), result);
        }

        [TestMethod]
        public void GetSmallIntegerAsBigInteger()
        {
            var dw = new DataWord(1);

            var result = dw.Value;

            Assert.IsNotNull(result);
            Assert.AreEqual(BigInteger.One, result);
        }

        [TestMethod]
        public void AddOneToOne()
        {
            var dw = new DataWord(1);

            var result = dw.Add(dw).Value;

            Assert.IsNotNull(result);
            Assert.AreEqual(new BigInteger(2), result);
        }

        [TestMethod]
        public void AddOneToMinusOne()
        {
            var dw = new DataWord(1);
            var dw2 = new DataWord(-1);

            var result = dw.Add(dw2).Value;

            Assert.IsNotNull(result);
            Assert.AreEqual(BigInteger.Zero, result);
        }

        [TestMethod]
        public void SubtractOneFromTwo()
        {
            var dw = new DataWord(2);

            var result = dw.Subtract(new DataWord(1)).Value;

            Assert.IsNotNull(result);
            Assert.AreEqual(BigInteger.One, result);
        }

        [TestMethod]
        public void SubtractMinusOneFromTwo()
        {
            var dw = new DataWord(2);

            var result = dw.Subtract(new DataWord(-1)).Value;

            Assert.IsNotNull(result);
            Assert.AreEqual(new BigInteger(3), result);
        }

        [TestMethod]
        public void MultiplyTwoByThree()
        {
            var dw = new DataWord(2);

            var result = dw.Multiply(new DataWord(3)).Value;

            Assert.IsNotNull(result);
            Assert.AreEqual(new BigInteger(6), result);
        }

        [TestMethod]
        public void DivideSixIntoThree()
        {
            var dw = new DataWord(6);

            var result = dw.Divide(new DataWord(3)).Value;

            Assert.IsNotNull(result);
            Assert.AreEqual(new BigInteger(2), result);
        }

        [TestMethod]
        public void Compare()
        {
            Assert.IsTrue(DataWord.Zero.Compare(DataWord.Zero) == 0);
            Assert.IsTrue(DataWord.Zero.Compare(DataWord.One) < 0);
            Assert.IsTrue(DataWord.One.Compare(DataWord.Zero) > 0);

            Assert.IsTrue(new DataWord(-1).Compare(new DataWord(-2)) > 0);
            Assert.IsTrue(new DataWord(-2).Compare(new DataWord(-1)) < 0);
            Assert.IsTrue(new DataWord(-2).Compare(new DataWord(-2)) == 0);
        }

        [TestMethod]
        public void And()
        {
            DataWord dw1 = new DataWord(new byte[] { 0x0f, 0xf0, 0x0f, 0xf0, 0x0f, 0xf0 });
            DataWord dw2 = new DataWord(new byte[] { 0xf1, 0x1f, 0xf1, 0x1f, 0xf1, 0x1f });
            DataWord dw3 = new DataWord(new byte[] { 0x01, 0x10, 0x01, 0x10, 0x01, 0x10 });

            Assert.AreEqual(dw3, dw1.And(dw2));
        }

        [TestMethod]
        public void Or()
        {
            DataWord dw1 = new DataWord(new byte[] { 0x0f, 0xf0, 0x0f, 0xf0, 0x0f, 0xf0 });
            DataWord dw2 = new DataWord(new byte[] { 0xf1, 0x1f, 0xf1, 0x1f, 0xf1, 0x1f });
            DataWord dw3 = new DataWord(new byte[] { 0xff, 0xff, 0xff, 0xff, 0xff, 0xff });

            Assert.AreEqual(dw3, dw1.Or(dw2));
        }

        [TestMethod]
        public void Xor()
        {
            DataWord dw1 = new DataWord(new byte[] { 0x0f, 0xf0, 0x0f, 0xf0, 0x0f, 0xf0 });
            DataWord dw2 = new DataWord(new byte[] { 0xf1, 0x1f, 0xf1, 0x1f, 0xf1, 0x1f });
            DataWord dw3 = new DataWord(new byte[] { 0xfe, 0xef, 0xfe, 0xef, 0xfe, 0xef });

            Assert.AreEqual(dw3, dw1.Xor(dw2));
        }

        [TestMethod]
        public void Not()
        {
            DataWord dw1 = new DataWord(new byte[] { 0x0f, 0xf0, 0x0f, 0xf0, 0x0f, 0xf0 });
            DataWord dw2 = new DataWord(new byte[] { 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xf0, 0x0f, 0xf0, 0x0f, 0xf0, 0x0f });

            Assert.AreEqual(dw2, dw1.Not());
        }
    }
}