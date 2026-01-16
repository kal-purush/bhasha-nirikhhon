using System;
using System.Reflection.PortableExecutable;
using Common.UserDto;
using UserApi.MappingProfiles;
using System.Text;
using Microsoft.Extensions.Configuration;
using System.Linq.Expressions;

namespace UnitTest.UserApi.Services
{
	public class UserServiceTest
	{
		private Mock<IUnitOfWork> _unitOfWork = new Mock<IUnitOfWork>();
		private Mock<ILogger<UserService>> _logger = new Mock<ILogger<UserService>>();
		private Mock<IConfiguration> _config;
		private UserService _userService;

		[SetUp]
		public void SetUp()
		{
            var myProfile = new EntitiesToDtosMappingProfile();
            var configuration = new MapperConfiguration(cfg => cfg.AddProfile(myProfile));
            IMapper mapper = new Mapper(configuration);

			_config = new Mock<IConfiguration>();

			InitConfig();
            InitData();

			_userService = new UserService(_unitOfWork.Object, mapper, _config.Object, _logger.Object);
        }

		private void InitConfig()
		{
            var saltSizeSection = new Mock<IConfigurationSection>();
            saltSizeSection.Setup(x => x.Value).Returns("32");
            _config.Setup(x => x.GetSection("SaltSize")).Returns(saltSizeSection.Object);

            var secretKeySection = new Mock<IConfigurationSection>();
            secretKeySection.Setup(x => x.Value).Returns("MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBAMnvSyE1UBKaiTtR7z/Ay73iwaWyeXX3");
            _config.Setup(x => x.GetSection("Jwt:SecretKey")).Returns(secretKeySection.Object);

            var issuerSection = new Mock<IConfigurationSection>();
            issuerSection.Setup(x => x.Value).Returns("UserApiServer");
            _config.Setup(x => x.GetSection("Jwt:Issuer")).Returns(issuerSection.Object);

            var audienceSection = new Mock<IConfigurationSection>();
            audienceSection.Setup(x => x.Value).Returns("ApiServer");
            _config.Setup(x => x.GetSection("Jwt:Audience")).Returns(audienceSection.Object);

            var expireSection = new Mock<IConfigurationSection>();
            expireSection.Setup(x => x.Value).Returns("240");
            _config.Setup(x => x.GetSection("Jwt:ExpireInMinutes")).Returns(expireSection.Object);
        }

        private void InitData()
        {
			var normalUser = new UserInfo
			{
				Id = new Guid("3cb57aa7-5dab-43ad-ba38-f0b4a49d4be6"),
				UserName = "NormalUser",
				Salt = "/8DwKbtaytBNT2gGKX2AA7CMeYX7OBNPkqwWZINfBKQSkZG82x6xRJmdbJl976iNfHuIryw9AQS0iKaaW1f5zs/s5Lxj6X5Q5naqeBhFm5IN1eh0cR0UfyLX1c6/WEZ0M0m+wZUE2BfR2fPYcZERkw9OL9KtUyALPYOnwn7GY++aqJemkO3J1SXbrWJ2wgA02ExS+f0uPg9xt98UxveuuRXqYxW/VJ1KDGXp+8I92SFW/X/VGKMyLaO4YGXl4hh2PfHWk0xemq+BimB4lZeKJpGhU+1C/pd1J3eo/IqKUoQlKDzs2m4ALFHJE+9cxLRnmwZpPidfrvuZZhznbs2D2Q==",
				HashedPassword = "YLDkU5EbQUm45YyuYsI5ttDqef/thIfdGueN0zJeMNw="
            };
			var normalRole = new Role
			{
				Id = new Guid("3d85e57b-89ef-47c4-a336-5a72d7073204"),
				Name = "User"
			};
			var userRoleNormal = new UserInfoRole
			{
				Id = new Guid("0db80a40-ae96-44c0-a07a-722f63db35be"),
				RoleId = new Guid("3d85e57b-89ef-47c4-a336-5a72d7073204"),
				UserInfoId = new Guid("3cb57aa7-5dab-43ad-ba38-f0b4a49d4be6")
			};

            _unitOfWork.Setup(x => x.UserInfo.Add(It.IsAny<UserInfo>())).Verifiable();
			_unitOfWork.Setup(x => x.UserInfoRole.Add(It.IsAny<UserInfoRole>())).Verifiable();

			_unitOfWork.Setup(x => x.UserInfo.Query())
				.Returns((new List<UserInfo> { normalUser }).AsQueryable<UserInfo>);
			_unitOfWork.Setup(x => x.Role.Query())
				.Returns(new List<Role> { normalRole }.AsQueryable());
			_unitOfWork.Setup(x => x.UserInfoRole.Query())
                .Returns(new List<UserInfoRole> { userRoleNormal }.AsQueryable<UserInfoRole>);
        }

        [Test]
		public void SignUp()
		{
			var dto = new UserInfoDto
			{
				UserName = "Test",
				Password = "Test"
			};

			var result = _userService.SignUp(dto);

			Assert.That(result, Is.EqualTo(true));
		}

		[Test]
		[TestCase("NormalUser", "NormalUser", true)]
		[TestCase("User2", "User2", false)]
		public void SignIn(string userName, string passWord, bool check)
		{
			var dto = new UserInfoDto
			{
				UserName = userName,
				Password = passWord
			};

			var result = !string.IsNullOrEmpty(_userService.SignIn(dto));

			Assert.That(result, Is.EqualTo(check));
		}
	}
}
