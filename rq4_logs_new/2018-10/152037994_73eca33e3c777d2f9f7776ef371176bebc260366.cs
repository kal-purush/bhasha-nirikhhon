using Cacti.Utils.AsyncUtil;
using Cacti.Utils.UnitTests.Mocks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cacti.Utils.UnitTests
{
    [TestClass]
    public class AsyncEnumerableUnitTest
    {
        [TestMethod]
        public async Task CachedEnumerable()
        {
            const int TOTAL = 15;

            PaginatedContext<string> paginatedContext = new PaginatedContext<string>(TOTAL, (index) => $"index: {index}");
            PaginatedAsyncEnumerable<string> paginatedAsync = new PaginatedAsyncEnumerable<string>(3, paginatedContext);

            IAsyncEnumerator<string> asyncEnumerator = paginatedAsync.GetAsyncEnumerator();

            List<string> values = new List<string>();
            while(await asyncEnumerator.MoveNextAsync(CancellationToken.None))
            {
                values.Add(asyncEnumerator.Current);
            }

            Assert.AreEqual(TOTAL, values.Count);
        }
    }
}