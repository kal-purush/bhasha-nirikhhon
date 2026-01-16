using Microsoft.VisualStudio.TestTools.UnitTesting;
using NetSort.Tests.Models;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NetSort.Tests
{
    [TestClass]
    public class ThenBySortableExtensionsTests
    {
        [TestMethod]
        public void ThenBySortableExtensions_SortByKey_SimpleSort_HasReasonablePerformance()
        {
            var people = new List<Person>();
            for (var index = 0; index < 100000; index++)
            {
                people.Add(new Person { Age = index % 100, Name = (index % 200).ToString() });
            }

            var sw = new Stopwatch();
            sw.Start();

            var sortedPeople = people
                .SortByKey("age")
                .ThenSortByKey("name")
                .ToList();

            sw.Stop();

            var isReasonable = sw.ElapsedMilliseconds <= 1000;

            Assert.IsTrue(isReasonable);
        }
    }
}