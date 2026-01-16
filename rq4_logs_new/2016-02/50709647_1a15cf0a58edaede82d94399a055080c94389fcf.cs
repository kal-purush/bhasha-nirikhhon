using System.Windows;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MineSearch.Wpf.Converters;

namespace MineSearch.Wpf.Test.ConverterTests
{
    [TestClass]
    public class InverseBooleanToVisibilityConverterTest
    {
        [TestMethod]
        public void TestVisibile()
        {
            var converter = new InverseBooleanToVisibilityConverter();

            Assert.AreEqual(Visibility.Visible, converter.Convert(false, typeof(Visibility), null, null));
        }

        [TestMethod]
        public void TestCollapsed()
        {
            var converter = new InverseBooleanToVisibilityConverter();

            Assert.AreEqual(Visibility.Collapsed, converter.Convert(true, typeof(Visibility), null, null));
        }
    }
}