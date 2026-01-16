using CourseGenerator.Models.Entities.Identity;
using Microsoft.AspNetCore.Identity;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CourseGenerator.DAL.Repositories
{
    public class ApplicationUserManager : UserManager<User>
    {
        public ApplicationUserManager(
            IUserStore<User> store, 
            IOptions<IdentityOptions> optionsAccessor, 
            IPasswordHasher<User> passwordHasher, 
            IEnumerable<IUserValidator<User>> userValidators, 
            IEnumerable<IPasswordValidator<User>> passwordValidators, 
            ILookupNormalizer keyNormalizer, 
            IdentityErrorDescriber errors, 
            IServiceProvider services, 
            ILogger<UserManager<User>> logger) : base(
                store, 
                optionsAccessor, 
                passwordHasher, 
                userValidators, 
                passwordValidators, 
                keyNormalizer, 
                errors, 
                services, 
                logger)
        {

        }

        /// <summary>
        /// Шукає користувача за номером телефону.
        /// </summary>
        /// <param name="phoneNumber">Номер телефону потрібного користувача</param>
        /// <returns>Користувач, асоційований з цим номером</returns>
        /// <remarks>
        /// <para>
        /// Необхідно впевнитись, що сутність <c>User</c> налаштована так, що
        /// кожен користувач має унікальний номер телефону.
        /// </para>
        /// <para>
        /// Якщо існує 2 користувача з таким номером то буде повернуто <c>null</c>.
        /// </para>
        /// </remarks>
        public async Task<User> FindByPhoneNumberAsync(string phoneNumber)
        {
            if (Users.Where(p => p.PhoneNumber == phoneNumber).Count() > 1)
                return null;

            return await Users.FirstOrDefaultAsync(p => p.PhoneNumber == phoneNumber);
        }
    }
}