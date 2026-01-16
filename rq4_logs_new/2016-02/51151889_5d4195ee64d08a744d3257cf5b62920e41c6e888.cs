namespace BoardGames.Web.Controllers.Tests
{
    using BoardGames.Controllers;
    using NUnit.Framework;
    using TestStack.FluentMVCTesting;

    [TestFixture]
    public class ErrorsControllerTests
    {
        private ErrorController errorsController;

        [TestFixtureSetUp]
        public void PlanetAreaControllerTestsSetup()
        {
            errorsController = new ErrorController();
        }

        [Test]
        public void TestBadRequestError()
        {
            errorsController
                .WithCallTo(x => x.BadRequest())
                .ShouldRenderView("BadRequest");
        }

        [Test]
        public void TestNotFoundError()
        {
            errorsController
                .WithCallTo(x => x.NotFound())
                .ShouldRenderView("NotFound");
        }

        [Test]
        public void TestInternalServerError()
        {
            errorsController
                .WithCallTo(x => x.Index())
                .ShouldRenderView("Index");
        }
    }
}