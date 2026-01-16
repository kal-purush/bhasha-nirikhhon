using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace EasyMicroservice.Utilities.Tests.Collections.Generic
{
    public class DictionaryExtensionsTest
    {
        [Theory]
        [InlineData("Ali", "Yousefi", true)]
        [InlineData("Mahdi", "Yousefi", false)]
        [InlineData("Mahdi", "Delzendeh", false)]
        public void TryAddItem(string key,string value, bool expect)
        {
            Dictionary<string, string> items = new Dictionary<string, string>();
            items.Add("Mahdi", "Delzendeh");
            Assert.Equal(expect, items.TryAddItem(key, value));
        }
    }
}