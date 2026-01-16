namespace MarketVault.Tests.UnitTests.HelpClassesTests
{
    using MarketVault.Core;
    using NUnit.Framework;

    /// <summary>
    /// Pager unit tests
    /// </summary>
    [TestFixture]
    public class PagerTests
    {
        /// <summary>
        /// Pager test
        /// </summary>
        [Test]
        public void Pager_ShouldWorkProperly()
        {
            var pager = new Pager(1, 1, 1)
            {
                Action = "Action",
                Controller = "Controller",
                SearchQuery = "Search",
                SearchSortingType = "SearchSortType",
                SearchViewName = "View"
            };

            Assert.Multiple(() =>
            {
                Assert.That(pager.Action == "Action");
                Assert.That(pager.Controller == "Controller");
                Assert.That(pager.SearchQuery == "Search");
                Assert.That(pager.SearchSortingType == "SearchSortType");
                Assert.That(pager.SearchViewName == "View");
                Assert.That(pager.TotalItems == 1);
                Assert.That(pager.PageSize == 1);
                Assert.That(pager.TotalPages == 1);
                Assert.That(pager.CurrentPage == 1);
                Assert.That(pager.StartPage == 1);
                Assert.That(pager.EndPage == 1);
            });

            var pager2 = new Pager(15, 15, 1)
            {

            };

            Assert.That(pager2.StartPage == 6);

            var pager3 = new Pager();

            Assert.Multiple(() =>
            {
                Assert.That(pager3.Action == "Index");
                Assert.That(pager3.Controller == "Home");
                Assert.That(pager3.SearchQuery == string.Empty);
                Assert.That(pager3.SearchSortingType == string.Empty);
                Assert.That(pager3.SearchViewName == string.Empty);
                Assert.That(pager3.TotalItems == 0);
                Assert.That(pager3.PageSize == 0);
                Assert.That(pager3.TotalPages == 0);
                Assert.That(pager3.CurrentPage == 0);
                Assert.That(pager3.StartPage == 0);
                Assert.That(pager3.EndPage == 0);
            });
        }
    }
}