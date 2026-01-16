using System;
using System.Collections.Generic;
using System.Reflection;
using System.Reflection.Metadata.Ecma335;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Jlw.Utilities.Data.Tests.UnitTests.DataUtilityStatic
{
    [TestClass]
    public class GenerateRandomFixture
    {
        [AttributeUsage(AttributeTargets.Method, AllowMultiple = true)]
        public class GenerateRandomTestDataAttribute : Attribute, ITestDataSource
        {
            private Type _type;

            public GenerateRandomTestDataAttribute(Type t)
            {
                _type = t;
            }


            public IEnumerable<object[]> GetData(MethodInfo methodInfo)
            {
                yield return new object[] {_type, null, null};
                if (_type != typeof(string) && _type != typeof(bool))    // GenerateRandom will return empty strings, so do not test these since it will cause unique tests to fail.
                {
                    if (_type != typeof(byte))
                        yield return new object[] {_type, null, 0};

                    yield return new object[] {_type, 0, null};
                }

                yield return new object[] {_type, sbyte.MinValue, sbyte.MaxValue };
                if (_type != typeof(bool))
                    yield return new object[] {_type, byte.MinValue, byte.MaxValue };
                yield return new object[] {_type, short.MinValue, short.MaxValue };
                yield return new object[] {_type, int.MinValue, int.MaxValue};
                yield return new object[] {_type, long.MinValue, long.MaxValue };
                yield return new object[] {_type, float.MinValue, float.MaxValue };
                yield return new object[] {_type, decimal.MinValue, decimal.MaxValue };
                yield return new object[] {_type, 0, 2};
                yield return new object[] {_type, 2, 0};

                //throw new NotImplementedException();

            }

            public string GetDisplayName(MethodInfo methodInfo, object[] data)
            {
                return $"{methodInfo.Name} ({DataUtility.GetTypeName((Type)data[0])}, {data[1] ?? "null"}, {data[2] ?? "null"})";
                //throw new NotImplementedException();
            }
        }


        [TestMethod]
        [GenerateRandomTestData(typeof(bool))]
        [GenerateRandomTestData(typeof(Byte))]
        [GenerateRandomTestData(typeof(SByte))]
        [GenerateRandomTestData(typeof(Int16))]
        [GenerateRandomTestData(typeof(Int32))]
        [GenerateRandomTestData(typeof(Int64))]
        [GenerateRandomTestData(typeof(DateTime))]
        [GenerateRandomTestData(typeof(Single))]
        [GenerateRandomTestData(typeof(Double))]
        [GenerateRandomTestData(typeof(Decimal))]
        [GenerateRandomTestData(typeof(String))]
        public void ShouldReturnRandomValue(Type t, object minLength, object maxLength)
        {
            object newVal = null;
            object prevVal = newVal;

            for (int i = 0; i < 5; i++)
            {
                prevVal = newVal;
                newVal = GetNewRandomValue(t, minLength, maxLength, prevVal);
                Console.WriteLine($"Previous Value: {prevVal ?? "<NULL>"}, New Value: {newVal ?? "<NULL>"}");
                Assert.AreNotEqual(prevVal, newVal, "Previous value should not match random value");
            }
        }

        [TestMethod]
        [GenerateRandomTestData(typeof(bool))]
        [GenerateRandomTestData(typeof(Byte))]
        [DataRow(typeof(string), null, 0, DisplayName = "ShouldReturnRandomValueOfType(string, null, 0)")]
        [GenerateRandomTestData(typeof(SByte))]
        [GenerateRandomTestData(typeof(Int16))]
        [GenerateRandomTestData(typeof(Int32))]
        [GenerateRandomTestData(typeof(Int64))]
        [GenerateRandomTestData(typeof(DateTime))]
        [GenerateRandomTestData(typeof(Single))]
        [GenerateRandomTestData(typeof(Double))]
        [GenerateRandomTestData(typeof(Decimal))]
        [GenerateRandomTestData(typeof(String))]
        [DataRow(typeof(string), 0, null, DisplayName = "ShouldReturnRandomValueOfType(string, 0, null)")]
        [DataRow(typeof(string), null, 0, DisplayName = "ShouldReturnRandomValueOfType(string, null, 0)")]

        [DataRow(typeof(Char), null, null, DisplayName = "ShouldReturnRandomValueOfType(char, null, null)")]

        public void ShouldReturnRandomValueOfType(Type t, object minLength, object maxLength)
        {
            object newVal = null;
            object prevVal = newVal;

            for (int i = 0; i < 5; i++)
            {
                prevVal = newVal;
                newVal = GetNewRandomValue(t, minLength, maxLength, prevVal);
                Assert.IsInstanceOfType(newVal, t);
                Console.WriteLine($"Previous Value: {prevVal ?? "<NULL>"}, New Value: {newVal ?? "<NULL>"}");
            }
        }

        [TestMethod]
        [GenerateRandomTestData(typeof(Byte))]
        [DataRow(typeof(string), null, 0, DisplayName = "ShouldReturnRandomValueOfType(string, null, 0)")]
        [GenerateRandomTestData(typeof(SByte))]
        [GenerateRandomTestData(typeof(Int16))]
        [GenerateRandomTestData(typeof(Int32))]
        [GenerateRandomTestData(typeof(Int64))] 
        [GenerateRandomTestData(typeof(Single))]
        [GenerateRandomTestData(typeof(Double))]
        [GenerateRandomTestData(typeof(Decimal))]
        [GenerateRandomTestData(typeof(String))]
        [DataRow(typeof(string), 0, null, DisplayName = "ShouldReturnRandomValueOfType(string, 0, null)")]
        [DataRow(typeof(string), null, 0, DisplayName = "ShouldReturnRandomValueOfType(string, null, 0)")]
        public void ShouldReturnRandomValueWithinConstraints(Type t, object minLength, object maxLength)
        {
            object newVal = null;
            object prevVal = newVal;

            for (int i = 0; i < 5; i++)
            {
                prevVal = newVal;
                newVal = GetNewRandomValue(t, minLength, maxLength, prevVal);
                Assert.IsInstanceOfType(newVal, t);
                Console.WriteLine($"Previous Value: {prevVal ?? "<NULL>"}, New Value: {newVal ?? "<NULL>"}");



                TypeCode ti = Type.GetTypeCode(t);
                object testVal;
                switch (Type.GetTypeCode(t))
                {
                    case TypeCode.Single:
                        float fVal, fMax, fMin;
                        //determine constraints
                        fMax = DataUtility.ParseNullableFloat(maxLength) ?? DataUtility.ParseFloat(t.GetTypeInfo().GetField("MaxValue")?.GetValue(default));
                        fMin = DataUtility.ParseNullableFloat(minLength) ?? DataUtility.ParseFloat(t.GetTypeInfo().GetField("MinValue")?.GetValue(default));
                        fVal = Math.Min(fMin, fMax);
                        fMax = Math.Max(fMin, fMax);
                        fMin = fVal;
                        fMin = Math.Max(fMin, DataUtility.ParseFloat(t.GetTypeInfo().GetField("MinValue")?.GetValue(default)));
                        fMax = Math.Min(fMax, DataUtility.ParseFloat(t.GetTypeInfo().GetField("MaxValue")?.GetValue(default)));

                        // Parse the new value int long for comparison
                        fVal = DataUtility.ParseFloat(newVal);
                        // Ensure that parsed value equals random value
                        Assert.AreEqual(newVal.ToString(), fVal.ToString());

                        // Random value should be less than/equal to maximum value
                        Assert.IsTrue(fVal >= fMin, $"Random value <{fVal}> is not less than/equal to minimum value specified <{fMin}>");
                        Console.WriteLine($"✓ {newVal} >= {fMin}");

                        // Random value should be less than/equal to maximum value
                        Assert.IsTrue(fVal <= fMax, $"Random value <{fVal}> is not less than/equal to maximum value specified <{fMax}>");
                        Console.WriteLine($"✓ {newVal} <= {fMax}");
                        break;
                    case TypeCode.Double:
                        double dVal, dMax, dMin;
                        //determine constraints
                        dMax = DataUtility.ParseNullableDouble(maxLength) ?? DataUtility.ParseDouble(t.GetTypeInfo().GetField("MaxValue")?.GetValue(default));
                        dMin = DataUtility.ParseNullableDouble(minLength) ?? DataUtility.ParseDouble(t.GetTypeInfo().GetField("MinValue")?.GetValue(default));
                        dVal = Math.Min(dMin, dMax);
                        dMax = Math.Max(dMin, dMax);
                        dMin = dVal;
                        dMin = Math.Max(dMin, DataUtility.ParseDouble(t.GetTypeInfo().GetField("MinValue")?.GetValue(default)));
                        dMax = Math.Min(dMax, DataUtility.ParseDouble(t.GetTypeInfo().GetField("MaxValue")?.GetValue(default)));

                        // Parse the new value int long for comparison
                        dVal = DataUtility.ParseDouble(newVal);
                        // Ensure that parsed value equals random value
                        Assert.AreEqual(newVal.ToString(), dVal.ToString());

                        // Random value should be less than/equal to maximum value
                        Assert.IsTrue(dVal >= dMin, $"Random value <{dVal}> is not less than/equal to minimum value specified <{dMin}>");
                        Console.WriteLine($"✓ {newVal} >= {dMin}");

                        // Random value should be less than/equal to maximum value
                        Assert.IsTrue(dVal <= dMax, $"Random value <{dVal}> is not less than/equal to maximum value specified <{dMax}>");
                        Console.WriteLine($"✓ {newVal} <= {dMax}");
                        break;
                    case TypeCode.Decimal:
                        decimal dcVal, dcMax, dcMin;
                        //determine constraints
                        dcMax = DataUtility.ParseNullableDecimal(maxLength) ?? DataUtility.ParseDecimal(t.GetTypeInfo().GetField("MaxValue")?.GetValue(default));
                        dcMin = DataUtility.ParseNullableDecimal(minLength) ?? DataUtility.ParseDecimal(t.GetTypeInfo().GetField("MinValue")?.GetValue(default));
                        dcVal = Math.Min(dcMin, dcMax);
                        dcMax = Math.Max(dcMin, dcMax);
                        dcMin = dcVal;
                        dcMin = Math.Max(dcMin, DataUtility.ParseDecimal(t.GetTypeInfo().GetField("MinValue")?.GetValue(default)));
                        dcMax = Math.Min(dcMax, DataUtility.ParseDecimal(t.GetTypeInfo().GetField("MaxValue")?.GetValue(default)));

                        // Parse the new value int long for comparison
                        dcVal = DataUtility.ParseDecimal(newVal);
                        // Ensure that parsed value equals random value
                        Assert.AreEqual(newVal.ToString(), dcVal.ToString());

                        // Random value should be less than/equal to maximum value
                        Assert.IsTrue(dcVal >= dcMin, $"Random value <{dcVal}> is not less than/equal to minimum value specified <{dcMin}>");
                        Console.WriteLine($"✓ {newVal} >= {dcMin}");

                        // Random value should be less than/equal to maximum value
                        Assert.IsTrue(dcVal <= dcMax, $"Random value <{dcVal}> is not less than/equal to maximum value specified <{dcMax}>");
                        Console.WriteLine($"✓ {newVal} <= {dcMax}");
                        break;
                    case TypeCode.Byte:
                    case TypeCode.SByte:
                    case TypeCode.Int16:
                    case TypeCode.Int32:
                    case TypeCode.Int64:
                        long lVal, lMax, lMin;
                        //determine constraints
                        lMax = DataUtility.ParseNullableLong(maxLength) ?? DataUtility.ParseLong(t.GetTypeInfo().GetField("MaxValue")?.GetValue(default));
                        lMin = DataUtility.ParseNullableLong(minLength) ?? DataUtility.ParseLong(t.GetTypeInfo().GetField("MinValue")?.GetValue(default));
                        lVal = Math.Min(lMin, lMax);
                        lMax = Math.Max(lMin, lMax);
                        lMin = lVal;
                        lMin = Math.Max(lMin, DataUtility.ParseLong(t.GetTypeInfo().GetField("MinValue")?.GetValue(default)));
                        lMax = Math.Min(lMax, DataUtility.ParseLong(t.GetTypeInfo().GetField("MaxValue")?.GetValue(default)));

                        // Parse the new value int long for comparison
                        lVal = DataUtility.ParseLong(newVal);
                        // Ensure that parsed value equals random value
                        Assert.AreEqual(newVal.ToString(), lVal.ToString());

                        // Random value should be less than/equal to maximum value
                        Assert.IsTrue(lVal >= lMin, $"Random value <{lVal}> is not less than/equal to minimum value specified <{lMin}>");
                        Console.WriteLine($"✓ {newVal} >= {lMin}");

                        // Random value should be less than/equal to maximum value
                        Assert.IsTrue(lVal <= lMax, $"Random value <{lVal}> is not less than/equal to maximum value specified <{lMax}>");
                        Console.WriteLine($"✓ {newVal} <= {lMax}");
                        break;
                    case TypeCode.String:
                        int nVal, nMax, nMin;
                        //determine constraints
                        nMax = (ushort?)DataUtility.ParseNullableInt(maxLength) ?? UInt16.MaxValue;
                        nMin = DataUtility.ParseNullableShort(minLength) ?? 10;
                        nVal = Math.Min(nMin, nMax);
                        nMax = Math.Max(nMin, nMax);
                        nMin = Math.Max(nVal, 0);
                        nMax = Math.Min(nMax, ushort.MaxValue);
                        nVal = newVal?.ToString()?.Length ?? 0;
                        Assert.IsTrue(nVal <= nMax, $"Random String length of {nVal} is greater than {nMax}");
                        Console.WriteLine($"✓ {nVal} <= {nMax}");
                        Assert.IsTrue(nVal >= nMin, $"Random String length of {nVal} is less than {nMin}");
                        Console.WriteLine($"✓ {nVal} >= {nMin}");
                        break;
                }
            }
        }

        [TestMethod]
        [DataRow(typeof(object), null, null, DisplayName = "ShouldReturnNullForNonValueTypes(object, null, null)")]
        public void ShouldReturnNullForNonValueTypes(Type t, object minLength, object maxLength)
        {
            object newVal = 0;
            for (int i = 0; i < 5; i++)
            {
                newVal = DataUtility.GenerateRandom(t, minLength, maxLength);
                Assert.IsNull(newVal);
            }
        }

        protected object GetNewRandomValue(Type t, object minLength, object maxLength, object prevValue)
        {
            object newVal = null;
            var n = 0;
            do
            {
                newVal = DataUtility.GenerateRandom(t, minLength, maxLength);
                //Console.WriteLine($"{newVal}");
            } while ((n++ < 100) && (newVal?.Equals(prevValue) ?? true));
            return newVal;
        }
        

    }
}