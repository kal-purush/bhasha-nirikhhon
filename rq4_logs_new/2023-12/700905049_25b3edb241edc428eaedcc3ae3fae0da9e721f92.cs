using Driving_School.Api.Tests.Infrastructures;
using Driving_School.Common.Entity.InterfaceDB;
using Driving_School.Context.Contracts.Interface;
using Xunit;

namespace Driving_School.Api.Tests
{
    /// <summary>
    /// Базовый класс для тестов
    /// </summary>
    [Collection(nameof(Driving_SchoolApiTestCollection))]
    public class BaseClassForIntegrationTests
    {
        protected readonly CustomWebApplicationFactory factory;
        protected readonly IDriving_SchoolContext context;
        protected readonly IUnitOfWork unitOfWork;

        public BaseClassForIntegrationTests(Driving_SchoolApiFixture fixture)
        {
            factory = fixture.Factory;
            context = fixture.Context;
            unitOfWork = fixture.UnitOfWork;
        }
    }
}