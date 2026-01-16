using System;
using TopTrumps.Data.Helpers;

namespace TopTrumps.Data.Tests.Services
{
    using System.Threading;
    using System.Threading.Tasks;
    using DTOs;
    using Microsoft.AspNetCore.Identity;
    using Moq;
    using Repository;
    using TopTrumps.Data.Services;
    using Xunit;

    public class UsersServiceTests
    {
        private readonly Mock<IRepository> _repo = new Mock<IRepository>();
        private readonly Mock<ISqlHelper> _helper = new Mock<ISqlHelper>();
        private readonly Mock<IRoleStore<Role>> _roleStore = new Mock<IRoleStore<Role>>();

        [Fact]
        public async Task CreateUser_ShouldCallRepoWithCorrectSql()
        {
            // arrange
            const string createUserSql = "createUserSql";
            const string addUserToRoleSql = "addUserToRoleSql";
            const string roleName = "Player";

            var user = new User();
            var role = new Role { NormalizedName = roleName };
            var token = new CancellationToken();

            _helper.Setup(x => x.GetCreateUsersSql())
                .Returns(createUserSql);

            _helper.Setup(x => x.GetAddUserToRoleSql(role))
                .Returns(addUserToRoleSql);

            _roleStore.Setup(x => x.FindByNameAsync(roleName, token))
                .ReturnsAsync(role);

            // act
            var actual = await GetSubject().CreateAsync(user, token);

            // assert
            Assert.True(actual == IdentityResult.Success);
            _helper.Verify(x => x.GetCreateUsersSql(), Times.Once);
            _helper.Verify(x => x.GetAddUserToRoleSql(role), Times.Once);
            _roleStore.Verify(x => x.FindByNameAsync(roleName, token), Times.Once);
            _repo.Verify(x => x.QuerySingleAsync<int>(createUserSql, token, user), Times.Once);
            _repo.Verify(x => x.ExecuteAsync(
                    addUserToRoleSql,
                    token,
                    It.Is<object>(o => o.GetHashCode() == (new { userId = user.Id, role.Id }).GetHashCode())),
                Times.Once);
        }

        [Fact]
        public async Task AddToRoleAsync_ShouldThrowException_IfRoleDoesNotExist()
        {
            // arrange
            const string createUserSql = "createUserSql";
            const string addUserToRoleSql = "addUserToRoleSql";
            const string roleName = "Player";

            var user = new User();
            var role = new Role { NormalizedName = roleName };
            var token = new CancellationToken();

            _helper.Setup(x => x.GetCreateUsersSql())
                .Returns(createUserSql);

            _helper.Setup(x => x.GetAddUserToRoleSql(role))
                .Returns(addUserToRoleSql);

            _roleStore.Setup(x => x.FindByNameAsync(roleName, token))
                .ReturnsAsync((Role)null);

            // act
            var ex = await Assert.ThrowsAsync<Exception>(() => GetSubject().AddToRoleAsync(user, roleName, token));

            // assert
            Assert.Equal(ex.Message, $"Role does not exist {roleName}");
        }

        [Fact]
        public async Task AddToRoleAsync_ShouldCallRepoWithCorrectSql()
        {
            // arrange
            const string createUserSql = "createUserSql";
            const string addUserToRoleSql = "addUserToRoleSql";
            const string roleName = "Player";

            var user = new User();
            var role = new Role { NormalizedName = roleName };
            var token = new CancellationToken();

            _roleStore.Setup(x => x.FindByNameAsync(roleName, token))
                .ReturnsAsync(role);

            _helper.Setup(x => x.GetCreateUsersSql())
                .Returns(createUserSql);

            _helper.Setup(x => x.GetAddUserToRoleSql(role))
                .Returns(addUserToRoleSql);

            // act
            await GetSubject().AddToRoleAsync(user, roleName, token);

            // assert
            _helper.Verify(x => x.GetAddUserToRoleSql(role), Times.Once);
            _roleStore.Verify(x => x.FindByNameAsync(roleName, token), Times.Once);
            _repo.Verify(x => x.ExecuteAsync(
                addUserToRoleSql, 
                token, 
                It.Is<object>(o => o.GetHashCode() == (new { userId = user.Id, role.Id }).GetHashCode())), 
                Times.Once);
        }

        private UsersService GetSubject()
        {
            return new UsersService(_repo.Object, _helper.Object, _roleStore.Object);
        }
    }
}