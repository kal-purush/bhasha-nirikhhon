using System;
using System.Text;
using System.Threading.Tasks;
using System.Security.Cryptography;
using AutoMapper;
using employers.application.Interfaces.UserAuth;
using employers.domain.Entities.UserAuth;
using employers.domain.Interfaces.Repositories.UserAuth;
using employers.domain.Requests;

namespace employers.application.UseCases.UserAuth
{
    public class CreateUserUseCaseAsync : ICreateUserUseCaseAsync
    {
        private readonly IMapper _mapper;
        private readonly IUserRepository _userRepository;
        private readonly IUserAuthRepository _userAuthRepository;

        public CreateUserUseCaseAsync(IMapper mapper,
                                      IUserRepository userRepository,
                                      IUserAuthRepository userAuthRepository)
        {
            _mapper = mapper;
            _userRepository = userRepository;
            _userAuthRepository = userAuthRepository;
        }

        public async Task<int> RunAsync(CreateUserRequest request)
        {
            // TODO Verify error automapper.;
            // var entity = _mapper.Map<UserEntity>(request) 
            var entity = new UserEntity
            {
                FullName = request.FullName,
                UserName = request.UserName,
                Password = request.Password
            };

            entity.Password = ComputeHash(request.Password, new SHA256CryptoServiceProvider());
            return await _userRepository.InsertUser(entity);
        }

        public string ComputeHash(string input, SHA256CryptoServiceProvider algorithm)
        {
            Byte[] inputBytes = Encoding.UTF8.GetBytes(input);
            Byte[] hashBytes = algorithm.ComputeHash(inputBytes);

            return BitConverter.ToString(hashBytes);
        }
    }
}