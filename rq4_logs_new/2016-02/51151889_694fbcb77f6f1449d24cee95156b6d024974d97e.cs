namespace BoardGames.Web.Routes.Tests
{
    using System.Web.Routing;
    using MvcRouteTester;
    using NUnit.Framework;
    using System.Web.Mvc;
    using System.Net.Http;
    using Areas.Admin;
    using Areas.Admin.Controllers;
    [TestFixture]
    public class AdminAreaRouteTests
    {
        private RouteCollection routeCollection;

        [TestFixtureSetUp]
        public void RouteCollectionSetup()
        {
            var areaReg = new AdminAreaRegistration();
            var areaContext = new AreaRegistrationContext(areaReg.AreaName, RouteTable.Routes);
            areaReg.RegisterArea(areaContext);

            RouteConfig.RegisterRoutes(RouteTable.Routes);

            this.routeCollection = RouteTable.Routes;
        }

        [TestFixtureTearDown]
        public void RouteCollectionDestroy()
        {
            RouteTable.Routes.Clear();
        }

        [Test]
        public void TestCommentsIndexRoute()
        {
            var url = "/Admin/Comments";
            this.routeCollection.ShouldMap(url).To<CommentsController>(HttpMethod.Get, c => c.Index());
        }

        [Test]
        public void TestReviewsIndexRoute()
        {
            var url = "/Admin/Reviews";
            this.routeCollection.ShouldMap(url).To<ReviewsController>(HttpMethod.Get, c => c.Index());
        }

        [Test]
        public void TestTutorialsIndexRoute()
        {
            var url = "/Admin/Tutorials";
            this.routeCollection.ShouldMap(url).To<TutorialsController>(HttpMethod.Get, c => c.Index());
        }

        [Test]
        public void TestUsersIndexRoute()
        {
            var url = "/Admin/Users";
            this.routeCollection.ShouldMap(url).To<UsersController>(HttpMethod.Get, c => c.Index());
        }

        [Test]
        public void TestCategoriesIndexRoute()
        {
            var url = "/Admin/Categories";
            this.routeCollection.ShouldMap(url).To<CategoriesController>(HttpMethod.Get, c => c.Index());
        }
    }
}