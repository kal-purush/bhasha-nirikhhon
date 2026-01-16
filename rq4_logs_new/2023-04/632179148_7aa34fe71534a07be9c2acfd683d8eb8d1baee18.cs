using System;
using Common.DoorDto;
using DoorApi.MappingProfiles;

namespace UnitTest.DoorApi.Services
{
	public class DoorServiceTest
	{
        private Mock<IUnitOfWork> _unitOfWork = new Mock<IUnitOfWork>();
        private Mock<ILogger<DoorService>> _logger = new Mock<ILogger<DoorService>>();
        private Mock<IConfiguration> _config;
        private DoorService _doorService;

        [SetUp]
        public void Setup()
		{
            var myProfile = new EntitiesToDtosMappingProfile();
            var configuration = new MapperConfiguration(cfg => cfg.AddProfile(myProfile));
            IMapper mapper = new Mapper(configuration);

            _config = new Mock<IConfiguration>();

            InitData();

            _doorService = new DoorService(_unitOfWork.Object, _logger.Object, mapper);
        }

        private void InitData()
        {
            var doors = new List<Door>
            {
                new Door {
                    Id = new Guid("2eee7efb-999a-489c-bd89-49911965a32d"),
                    Name = "FrontDoor",
                    Description = "Front Door"
                },
                new Door
                {
                    Id = new Guid("9216c4eb-a0b1-499e-b707-1541662b0ff8"),
                    Name = "StorageDoor",
                    Description = "Storage Door"
                }
            }.AsQueryable<Door>;
            var users = new List<UserInfo>
            {
                new UserInfo
                {
                    Id = new Guid("3cb57aa7-5dab-43ad-ba38-f0b4a49d4be6"),
                    UserName = "NormalUser"
                },
                new UserInfo
                {
                    Id = new Guid("a40270c3-6040-4239-98a1-2f94d9d79351"),
                    UserName = "AdminUser"
                }
            }.AsQueryable<UserInfo>;
            var roles = new List<Role>
            {
                new Role
                {
                    Id = new Guid("3d85e57b-89ef-47c4-a336-5a72d7073204"),
                    Name = "User"
                },
                new Role
                {
                    Id = new Guid("1202e0f8-0095-4cee-9145-ee9990c2ce0d"),
                    Name = "Admin"
                }
            }.AsQueryable<Role>;
            var userRoles = new List<UserInfoRole>
            {
                new UserInfoRole
                {
                    Id = new Guid("0db80a40-ae96-44c0-a07a-722f63db35be"),
                    UserInfoId = new Guid("3cb57aa7-5dab-43ad-ba38-f0b4a49d4be6"),
                    RoleId = new Guid("3d85e57b-89ef-47c4-a336-5a72d7073204")
                },
                new UserInfoRole
                {
                    Id = new Guid("eeb99975-bd6e-4195-843b-cc96d8ff7391"),
                    UserInfoId = new Guid("a40270c3-6040-4239-98a1-2f94d9d79351"),
                    RoleId = new Guid("1202e0f8-0095-4cee-9145-ee9990c2ce0d")
                }
            }.AsQueryable<UserInfoRole>;
            var doorRoles = new List<DoorRole>
            {
                new DoorRole
                {
                    Id = new Guid("c7d0fb9f-e716-4267-8286-04175e0ec44d"),
                    DoorId = new Guid("9216c4eb-a0b1-499e-b707-1541662b0ff8"),
                    RoleId = new Guid("1202e0f8-0095-4cee-9145-ee9990c2ce0d")
                },
                new DoorRole
                {
                    Id = new Guid("4d47aad8-fb7a-4f1b-a641-5e0afb33d01c"),
                    DoorId = new Guid("2eee7efb-999a-489c-bd89-49911965a32d"),
                    RoleId = new Guid("3d85e57b-89ef-47c4-a336-5a72d7073204")
                },
                new DoorRole
                {
                    Id = new Guid("c12278dd-b8df-426c-9b94-9a2c239b5f06"),
                    DoorId = new Guid("2eee7efb-999a-489c-bd89-49911965a32d"),
                    RoleId = new Guid("1202e0f8-0095-4cee-9145-ee9990c2ce0d")
                }
            }.AsQueryable<DoorRole>;

            _unitOfWork.Setup(x => x.Door.Query()).Returns(doors);
            _unitOfWork.Setup(x => x.UserInfo.Query()).Returns(users);
            _unitOfWork.Setup(x => x.Role.Query()).Returns(roles);
            _unitOfWork.Setup(x => x.UserInfoRole.Query()).Returns(userRoles);
            _unitOfWork.Setup(x => x.DoorRole.Query()).Returns(doorRoles);

            _unitOfWork.Setup(x => x.Door.Add(It.IsAny<Entities.Door>())).Verifiable();
        }

        [Test]
        [TestCase("FrontDoor", "Front Door", false)]
        [TestCase("Side Door", "Side Door", true)]
        public void CreateDoorTest(string doorName, string description, bool check)
        {
            var dto = new DoorDto()
            {
                Name = doorName,
                Description = description
            };

            var result = _doorService.CreateDoor(dto);

            Assert.That(result, Is.EqualTo(check));
        }

        [Test]
        [TestCase("FrontDoor", "NormalUser", true)]
        public void OpenDoor(string doorName, string userName, bool check)
        {
            var dto = new TapDoorDto
            {
                DoorName = doorName,
                UserName = userName,
                TapAction = "TAPIN"
            };

            var result = _doorService.Open(dto).Result;

            Assert.That(result, Is.EqualTo(check));
        }
	}
}
