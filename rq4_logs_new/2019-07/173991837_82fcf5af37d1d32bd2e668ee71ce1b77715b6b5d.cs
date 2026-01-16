using AutoFixture;
using AutoMapper;
using Crm.Application.Interface;
using Crm.Application.ViewModels.PermissionViewModels;
using Crm.Domain.Enum;
using Crm.Domain.Interfaces.Services;
using Crm.Domain.Models.Permission;
using Crm.Infra.CrossCutting.Identity.Interfaces;
using Microsoft.AspNetCore.Identity;
using Moq;
using System.Collections.Generic;
using System.Linq;
using System.Security.Claims;
using Xunit;

namespace Crm.Test
{
    public class PermissionAppServiceTest
    {
        private readonly IPermissionAppService _permissionAppServiceMock;
        private readonly IPermissionService _permissionServiceMock;
        private readonly IIdentityRepository _identityRepositoryMock;
        private readonly IMapper _mapperMock;

        private Role _role;
        private IEnumerable<Role> _roleList;
        private IEnumerable<RoleViewModel> _roleViewModel;

        private readonly Fixture _fixture;

        public PermissionAppServiceTest()
        {
            _fixture = new Fixture();

            var permissionAppServiceMock = new Mock<IPermissionAppService>();

            var mapperMock = new Mock<IMapper>();
            _mapperMock = mapperMock.Object;

            ConfigureRoleList();

            var permissionServiceMock = new Mock<IPermissionService>();

            permissionServiceMock
                .Setup(x => x.ComposeRole(It.IsAny<IdentityRole>(), It.IsAny<IEnumerable<Claim>>()))
                .Returns(ConfigureRole());

            _permissionServiceMock = permissionServiceMock.Object;

            var identityRepositoryMock = new Mock<IIdentityRepository>();

            identityRepositoryMock
                .Setup(x => x.GetAllRoles())
                .Returns(ConfigureIdentityRole());

            _identityRepositoryMock = identityRepositoryMock.Object;

            _permissionAppServiceMock = permissionAppServiceMock.Object;
        }

        private void ConfigureRoleList(IEnumerable<Role> roles = null)
        {
            _role = ConfigureRole();

            if (roles == null)
            {
                roles = new List<Role>
                {
                    ConfigureRole(),
                    _role
                };
            }

            _roleViewModel = _mapperMock.Map<IEnumerable<RoleViewModel>>(roles);
        }

        private Role ConfigureRole()
        {
            var roleClaims = new List<Claim>
            {
                new Claim(_fixture.Create<string>(), _fixture.Create<string>()),
                new Claim(CustomClaimTypes.Menu, _fixture.Create<string>())
            };

            return new Role(ConfigureIdentityRole().First(), roleClaims);
        }

        private IEnumerable<IdentityRole> ConfigureIdentityRole()
        {
            return new List<IdentityRole>
            {
                new IdentityRole(_fixture.Create<string>()),
                new IdentityRole(_fixture.Create<string>())
            };
        }

        [Fact]
        public static void ConstructorTest()
        {
            Assert.NotNull(1);
        }

        [Fact]
        public void Deve_Retornar_Todas_As_Permissoes()
        {
            var roles = _permissionAppServiceMock.ListAllRoles();

            Assert.Equal(new List<RoleViewModel>(), roles);
        }
    }
}