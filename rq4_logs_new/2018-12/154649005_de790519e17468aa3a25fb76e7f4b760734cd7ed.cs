using Microsoft.AspNet.Identity;
using Microsoft.AspNet.Identity.EntityFramework;
using Microsoft.AspNet.Identity.Owin;
using Microsoft.Owin;
using Microsoft.Owin.Security;
using Simply_Gallery.Models;
using System.Linq;
using System.Security.Claims;
using System.Threading.Tasks;

namespace Simply_Gallery.App_Start
{
    // настройка диспетчера пользователей
    // UserManager определен в ASP.NET Identity
    public class ApplicationUserManager : UserManager<ApplicationUser>
    {
        public ApplicationUserManager(IUserStore<ApplicationUser> store)
            : base(store)
        {
        }

        public static ApplicationUserManager Create(IdentityFactoryOptions<ApplicationUserManager> options,
                                                IOwinContext context)
        {
            // получаем контекст бд
            var db = context.Get<ApplicationContext>();
            
            // создаем менеджера для данного контекста
            var manager = new ApplicationUserManager(new UserStore<ApplicationUser>(db));

            // настройка валидации для пользователя
            manager.UserValidator = new ApplicationUserValidator(manager);

            // настройка валидации для пароля
            manager.PasswordValidator = new PasswordValidator
            {
                // Необходимая длина пароля
                RequiredLength = 6
            };

            return manager;
        }
    }

    // настройка диспетчера входа
    public class ApplicationSignInManager : SignInManager<ApplicationUser, string>
    {
        public ApplicationSignInManager(ApplicationUserManager userManager, IAuthenticationManager authenticationManager)
            : base(userManager, authenticationManager)
        {
        }

        public override Task<ClaimsIdentity> CreateUserIdentityAsync(ApplicationUser user)
        {
            return user.GenerateUserIdentityAsync((ApplicationUserManager)UserManager);
        }

        public static ApplicationSignInManager Create(IdentityFactoryOptions<ApplicationSignInManager> options, IOwinContext context)
        {
            return new ApplicationSignInManager(context.GetUserManager<ApplicationUserManager>(), context.Authentication);
        }
    }

    // настройка диспечера ролей
    public class ApplicationRoleManager : RoleManager<ApplicationRole>
    {
        public ApplicationRoleManager(RoleStore<ApplicationRole> store)
            : base(store)
        {
        }

        public static ApplicationRoleManager Create(IdentityFactoryOptions<ApplicationRoleManager> options,
                                                IOwinContext context)
        {
            return new ApplicationRoleManager(new
                    RoleStore<ApplicationRole>(context.Get<ApplicationContext>()));
        }
    }

    // кастомная валидация пользователей
    public class ApplicationUserValidator : UserValidator<ApplicationUser>
    {
        public ApplicationUserValidator(ApplicationUserManager manager)
            : base(manager)
        {
            AllowOnlyAlphanumericUserNames = true;  // юзернейм должен содержать только алфавитно-цифровые символы
            RequireUniqueEmail = true;              // email пользователя должен быть уникальным
        }

        public override async Task<IdentityResult> ValidateAsync(ApplicationUser user)
        {
            var nameBanned = new string[]
            {
                "admin", "админ", "user", "пользователь"
            };

            var emailBannder = new string[]
            {
                "@spam.com", "@temp.ru"
            };

            var result = await base.ValidateAsync(user);

            if (nameBanned.FirstOrDefault(x => user.UserName.ToLower().Contains(x)) != null)
            {
                var errors = result.Errors.ToList();

                errors.Add("Имя пользователя содержит недопустимые слова");
                result = new IdentityResult(errors);
            }

            if (emailBannder.FirstOrDefault(x => user.Email.ToLower().Contains(x)) != null)
            {
                var errors = result.Errors.ToList();

                errors.Add("Данный e-mail домен находится в спам зоне");
                result = new IdentityResult(errors);
            }

            return result;
        }
    }
}