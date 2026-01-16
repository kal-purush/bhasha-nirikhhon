using Moq;
using Rtl.TvMazeScrapper.Domain.Entity;
using Rtl.TvMazeScrapper.Domain.Interfaces.Repository;

namespace Rtl.TvMazeScrapper.Tests.UnitTests.RepositoryTests;
//A A A
public class CastRepositoryTest
{
        private Mock<ICastRepository<Cast>> _mockedCastRepository;

        [SetUp]
        public void Setup()
        {
            _mockedCastRepository = new Mock<ICastRepository<Cast>>();
        }

        [Test]
        public async Task Cast_Should_Be_AddedToTheDatabase()
        {
            var castEntity = new Cast(Guid.NewGuid().ToString(), DateTime.UtcNow, 1);
            _mockedCastRepository.Setup(s => s.AddAsync(castEntity)).ReturnsAsync(() => true);
            
            var cast = await _mockedCastRepository.Object.AddAsync(castEntity);
            
            _mockedCastRepository.Verify(m => m.AddAsync(It.IsAny<Cast>()), Times.Once());
        }

        [Test]
        public async Task TestToVerifyCastsInRepositoryWithAnyAsync()
        {
            _mockedCastRepository.Setup(s 
                => s.AnyAsync(a => a.Name == Guid.NewGuid().ToString())).ReturnsAsync(() => true);
            
            var cast = await _mockedCastRepository.Object.AnyAsync(a => a.Name == Guid.NewGuid().ToString());

            _mockedCastRepository.Verify(m 
                => m.AnyAsync(It.IsAny<System.Linq.Expressions.Expression<Func<Cast, bool>>>()), Times.Once());
        }

        [Test]
        public async Task Test_ToVerify_Cast_Returned_From_Repository_Async()
        {
            var castEntity = new Cast(Guid.NewGuid().ToString(), DateTime.UtcNow, 1);
            _mockedCastRepository.Setup(s => s.GetAsync(a => a.Name == Guid.NewGuid().ToString())).ReturnsAsync(() => castEntity);
            
            var cast = await _mockedCastRepository.Object.GetAsync(a => a.Name == Guid.NewGuid().ToString());
            
            _mockedCastRepository.Verify(m => m.GetAsync(It.IsAny<System.Linq.Expressions.Expression<Func<Cast, bool>>>()), Times.Once());
        }

        [Test]
        public async Task Test_ToVerify_Cast_Count_In_Repository_Async()
        {
            var count = 1;
            _mockedCastRepository.Setup(s => s.CountAsync()).ReturnsAsync(() => count);

            var cast = await _mockedCastRepository.Object.CountAsync();
            
            _mockedCastRepository.Verify(m => m.CountAsync(), Times.Once());
        }
}