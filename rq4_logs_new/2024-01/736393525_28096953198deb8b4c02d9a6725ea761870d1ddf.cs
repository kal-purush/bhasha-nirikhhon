using AutoMapper;
using BusinessLogic.Interfaces;
using BusinessLogic.Models;
using BusinessLogic.Models.DTOs;
using BusinessLogic.Utils;
using Data.Data;
using Data.Entities;
using Data.Interfaces;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BusinessLogic.Services
{
    public class AuthenticationService: IAuthenticationService
    {
        private readonly IAuthenticationValidator _authenticationValidator;
        private readonly IUnitOfWork _unitOfWork;
        private readonly IMapper _mapper;
        public AuthenticationService(IAuthenticationValidator authenticationValidator, IUnitOfWork unitOfWork, IMapper mapper)
        {
            _authenticationValidator = authenticationValidator;
            _unitOfWork = unitOfWork;
            _mapper = mapper;
        }

        public async Task<AuthenticationResult> SignIn(UserCredentials userCredentials)
        {
            var authenticationResult = new AuthenticationResult();
            var validationResult = await _authenticationValidator.ValidateForSignInAsync(userCredentials);
            if(!validationResult.IsValid)
            {
                authenticationResult.ErrorMessage = string.Join(", ", validationResult.Messages);
                authenticationResult.IsSuccessful = false;
                return authenticationResult;
            }
           
            var userToLogIn = (await _unitOfWork.UserRepository.GetAllAsync()).FirstOrDefault(x => x.Username.Equals(userCredentials.Username));
            authenticationResult.IsSuccessful = true;
            authenticationResult.User = _mapper.Map<UserDTO>(userToLogIn);

            return authenticationResult;
        }
    }
}