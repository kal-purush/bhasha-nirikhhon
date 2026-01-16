using Microsoft.VisualStudio.TestTools.UnitTesting;
using MineSearch.Wpf.Converters;

namespace MineSearch.Wpf.Test.ConverterTests
{
    [TestClass]
    public class AdjacentMineCountToStringConverterTest
    {
        [TestMethod]
        public void TestAdjacentMineCountString()
        {
            var converter = new AdjacentMineCountToStringConverter();

            // 0 should be empty
            Assert.AreEqual(string.Empty, converter.Convert(0, typeof(int), null, null));

            // Non zero
            Assert.AreEqual("8", converter.Convert(8, typeof(int), null, null));
        }
    }
}