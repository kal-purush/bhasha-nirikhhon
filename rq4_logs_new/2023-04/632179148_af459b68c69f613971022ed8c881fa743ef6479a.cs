using System;
using AdminApi.MappingProfiles;
using Common.AdminDto;

namespace UnitTest.AdminApi.Services
{
	public class DoorRoleServiceTest
	{
        private Mock<IUnitOfWork> _unitOfWork = new Mock<IUnitOfWork>();
        private Mock<ILogger<DoorRoleService>> _logger = new Mock<ILogger<DoorRoleService>>();
        private DoorRoleService _doorRoleService;

        [SetUp]
        public void Setup()
        {
            var myProfile = new EntitiesToDtosMappingProfile();
            var configuration = new MapperConfiguration(cfg => cfg.AddProfile(myProfile));
            IMapper mapper = new Mapper(configuration);

            InitData();

            _doorRoleService = new DoorRoleService(_unitOfWork.Object, _logger.Object, mapper);
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
                },
                new Door
                {
                    Id = new Guid("3d85e57b-89ef-47c4-a336-5a72d7073204"),
                    Name = "SideDoor",
                    Description = "Side Door"
                }
            }.AsQueryable<Door>;
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
            _unitOfWork.Setup(x => x.Role.Query()).Returns(roles);
            _unitOfWork.Setup(x => x.DoorRole.Query()).Returns(doorRoles);
        }

        [Test]
        [TestCase("SideDoor", "Admin", true)]
        [TestCase("FrontDoor", "User", false)]
        [TestCase("NonExistDoor", "User", false)]
        [TestCase("FrontDoor", "NonExistUser", false)]
        public void AssignDoorRoleTest(string doorName, string roleName, bool check)
        {
            var dto = new DoorRoleDto
            {
                DoorName = doorName,
                RoleName = roleName
            };

            var result = _doorRoleService.AssignDoorRole(dto);

            Assert.That(result, Is.EqualTo(check));
        }
    }
}
