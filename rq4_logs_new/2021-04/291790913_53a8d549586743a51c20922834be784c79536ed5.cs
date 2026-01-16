using LT.DigitalOffice.Broker.Requests;
using LT.DigitalOffice.Kernel.Broker;
using LT.DigitalOffice.Kernel.Exceptions.Models;
using LT.DigitalOffice.UserService.Business.Commands.Credentials.Interfaces;
using LT.DigitalOffice.UserService.Business.Helpers.Password;
using LT.DigitalOffice.UserService.Data.Interfaces;
using LT.DigitalOffice.UserService.Mappers.Db.Interfaces;
using LT.DigitalOffice.UserService.Models.Dto.Requests.Credentials;
using LT.DigitalOffice.UserService.Models.Dto.Responses.Credentials;
using MassTransit;
using Microsoft.Extensions.Logging;
using System;

namespace LT.DigitalOffice.UserService.Business.Commands.Credentials
{
    public class CreateCredentialsCommand : ICreateCredentialsCommand
    {
        private readonly IDbUserCredentialsMapper _mapper;
        private readonly IUserRepository _userRepository;
        private readonly IUserCredentialsRepository _userCredentialsRepository;
        private readonly IRequestClient<IGetTokenRequest> _rcToken;
        private readonly ILogger<CreateCredentialsCommand> _logger;

        public CreateCredentialsCommand(
            IDbUserCredentialsMapper mapper,
            IUserRepository userRepository,
            IUserCredentialsRepository userCredentialsRepository,
            IRequestClient<IGetTokenRequest> rcToken,
            ILogger<CreateCredentialsCommand> logger)
        {
            _mapper = mapper;
            _userRepository = userRepository;
            _userCredentialsRepository = userCredentialsRepository;
            _rcToken = rcToken;
            _logger = logger;
        }

        public CredentialsResponse Execute(CreateCredentialsRequest request)
        {
            // TODO add request validation

            if (request == null)
            {
                throw new BadRequestException();
            }

            var dbPendingUser = _userRepository.GetPendingUser(request.UserId);
            if (dbPendingUser == null)
            {
                throw new NotFoundException($"Pending user with ID '{request.UserId}' was not found.");
            }

            if (request.Password != dbPendingUser.Password)
            {
                throw new ForbiddenException();
            }

            try
            {
                IOperationResult<string> response = _rcToken.GetResponse<IOperationResult<string>>(
                    IGetTokenRequest.CreateObj(request.UserId)).Result.Message;

                if (response.IsSuccess && !string.IsNullOrEmpty(response.Body))
                {
                    _userRepository.DeletePendingUser(request.UserId);

                    string salt = $"{Guid.NewGuid()}{Guid.NewGuid()}";

                    string passwordHash = UserPasswordHash.GetPasswordHash(request.Login, salt, request.Password);

                    _userCredentialsRepository.Create(_mapper.Map(request, salt, passwordHash));

                    _userRepository.SwitchActiveStatus(request.UserId, true);

                    return new CredentialsResponse
                    {
                        UserId = request.UserId,
                        Token = response.Body
                    };
                }
                else
                {
                    _logger.LogWarning(
                        $"Can not get token for pending user '{request.UserId}' reason:{Environment.NewLine} '{string.Join('\n',response.Errors)}'");
                }
            }
            catch (Exception exc)
            {
                _logger.LogError(exc, "Something went wrong while we were creating the user credentials.");
            }

            throw new InvalidOperationException();
        }
    }
}