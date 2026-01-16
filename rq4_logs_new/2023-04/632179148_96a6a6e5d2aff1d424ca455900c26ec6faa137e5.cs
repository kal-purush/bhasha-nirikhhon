using System;
using HistoryApi.MappingProfiles;
using HistoryApi.Implementations;

namespace UnitTest.InOutHistoryApi.Services
{
	public class InOutHistoryServiceTest
	{
        private Mock<IUnitOfWork> _unitOfWork = new Mock<IUnitOfWork>();
        private Mock<ILogger<InOutHistoryService>> _logger = new Mock<ILogger<InOutHistoryService>>();
        private Mock<IConfiguration> _config;
        private InOutHistoryService _historyService;

        [SetUp]
        public void Setup()
        {
            var myProfile = new DtosToViewModelsMappingProfile();
            var configuration = new MapperConfiguration(cfg => cfg.AddProfile(myProfile));
            IMapper mapper = new Mapper(configuration);

            _config = new Mock<IConfiguration>();

            InitData();

            _historyService = new InOutHistoryService(_unitOfWork.Object, _logger.Object, mapper);
        }

        private void InitData()
        {

        }
    }
}
