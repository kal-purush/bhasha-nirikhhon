using Application.Repository;
using Common.Dtos;
using Common.Entities;
using Common.Exceptions;
using MediatR;
using Microsoft.Extensions.Logging;

namespace Application.Handlers.ApplicationUsers
{
    public class LogoutHandler : IRequestHandler<LogoutRequest, LogoutResponseDto>
    {
        private readonly ILogger<LogoutHandler> _logger;
        private readonly IRepository<SecurityTokenLog> _repository;
        private readonly IApplicationUserRepository _userRepository;

        public LogoutHandler(ILogger<LogoutHandler> logger, 
            IRepository<SecurityTokenLog> repository,
            IApplicationUserRepository userRepository)
        {
            _logger = logger;
            _repository = repository;
            _userRepository = userRepository;
        }

        public async Task<LogoutResponseDto> Handle(LogoutRequest request, CancellationToken cancellationToken)
        {
            var errorMessage = "User not validated.";
            var user = _userRepository.Find(u => u.UserId == request.UserId && u.IsEnabled == true).FirstOrDefault();
            
            if (user == null)
            {
                throw new InvalidModelException(errorMessage);
            }

            var tokenLogs = _repository.Find(l => l.UserId ==  request.UserId && l.IsEnabled == true);
            if (tokenLogs != null && tokenLogs.Count() > 0)
            {
                foreach (var tokenLog in tokenLogs)
                {
                    tokenLog.Disable();
                }
                await _repository.UpdateRangeAsync(tokenLogs);
            }

            LogoutResponseDto responseDto = new LogoutResponseDto();
            responseDto.Message = "Logout successful";
            return responseDto;
        }
    }
}