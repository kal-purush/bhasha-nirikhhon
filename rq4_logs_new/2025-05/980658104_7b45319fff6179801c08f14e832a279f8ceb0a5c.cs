using LocalAccountServiceProvider.Data.Contexts;
using LocalAccountServiceProvider.Data.Entities;
using LocalAccountServiceProvider.Services;
using Microsoft.AspNetCore.Identity;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Tests
{
    public class AccountService_Tests
    {
        #region Configurations

        private readonly ServiceProvider _provider;
        private readonly UserManager<AppUserEntity> _userManager;
        private readonly AuthenticationContext _context;
        private readonly IAccountService _accountService;
        private readonly RoleManager<IdentityRole> _roleManager;

        public AccountService_Tests()
        {
            var services = new ServiceCollection();

            services.AddDbContext<AuthenticationContext>(x => x.UseInMemoryDatabase($"{Guid.NewGuid()}"));

            services.AddIdentity<AppUserEntity, IdentityRole>(x =>
            {
                x.User.RequireUniqueEmail = true;
                x.Password.RequiredLength = 8;
                x.SignIn.RequireConfirmedPhoneNumber = true;
            })
                .AddEntityFrameworkStores<AuthenticationContext>()
                .AddRoleManager<RoleManager<IdentityRole>>();

            // Bygger services (typ motsvarande ** var app = builder.Build ** i program.cs)
            _provider = services.BuildServiceProvider();

            // Sätter värden till instansvariabler
            _userManager = _provider.GetRequiredService<UserManager<AppUserEntity>>();
            _context = _provider.GetRequiredService<AuthenticationContext>();
            _roleManager = _provider.GetRequiredService<RoleManager<IdentityRole>>();
            _accountService = new AccountService(_userManager, _roleManager);
        }

        public void Dispose()
        {
            _context.Database.EnsureDeleted();
            _provider.Dispose();
        }
    }

    #endregion Configurations

    //[Fact]
    //    public async Task CreateAccount_ShouldReturnSuccess_WhenValidDataProvided()
    //    {
    //        // arrange
    //        var request = new CreateAccountRequest
    //        {
    //            Email = "test@domain.com",
    //            Password = "Bytmig123!"
    //        };

    //        // act

    //        var response = await _accountService.CreateAccount(request);
    //    }
    //}
}