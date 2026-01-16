using BusinessLogic.Interfaces;
using BusinessLogic.Models;
using Data.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BusinessLogic.Validation
{
    public class AuthenticationValidator: IAuthenticationValidator
    {
        protected readonly IUnitOfWork _unitOfWork;
        protected readonly ISecurePasswordHasher _passwordHasher;
        public AuthenticationValidator(IUnitOfWork unitOfWork, ISecurePasswordHasher passwordHasher)
        {
            _unitOfWork = unitOfWork;
            _passwordHasher = passwordHasher;
        }

        public async Task<ValidationResult> ValidateForSignInAsync(UserCredentials userCredentials)
        {
            var result = new ValidationResult()
            {
                IsValid = true,
                Messages = new List<string>()
            };

            if (string.IsNullOrWhiteSpace(userCredentials.Username))
            {
                result.IsValid = false;
            }
            else if (string.IsNullOrWhiteSpace(userCredentials.Password))
            {
                result.IsValid = false;
            }
            else
            {
                var user = (await _unitOfWork.UserRepository.GetAllAsync()).FirstOrDefault(x => x.Username.Equals(userCredentials.Username));
                if (user == null)
                {
                    result.IsValid = false; 
                }
                else 
                {
                    var isVerified = _passwordHasher.Verify(userCredentials.Password, user.Password);
                    if (!isVerified)
                    {
                        result.IsValid = false;
                    }
                }

            }

            if(!result.IsValid)
            {
                result.Messages.Add("Username or password is incorrect");
            }

            return result;
        }
    }
}