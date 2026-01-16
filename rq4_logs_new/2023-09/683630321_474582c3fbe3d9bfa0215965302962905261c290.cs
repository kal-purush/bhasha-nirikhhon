using Application.Repository;
using Common.Dtos;
using Common.Entities;
using Common.Mappings;
using MediatR;
using Microsoft.Extensions.Logging;

namespace Application.Handlers.ApplicationUsers
{
    public class UpdateProfileHandler : IRequestHandler<UpdateProfileCommand, ApplicationUserResponseDto>
    {
        private readonly IRepository<ApplicationUserDetails> _repository;
        private readonly SharedMapping _mapping;
        private readonly ILogger<UpdateProfileHandler> _logger;
        private readonly IMediator _mediator;

        public UpdateProfileHandler(
            IRepository<ApplicationUserDetails> repository, 
            SharedMapping mapping, 
            IMediator mediator,
            ILogger<UpdateProfileHandler> logger)
        {
            _repository = repository;
            _mapping = mapping;
            _mediator = mediator;
            _logger = logger;
        }

        public async Task<ApplicationUserResponseDto> Handle(UpdateProfileCommand request, CancellationToken cancellationToken)
        {
            _logger.LogInformation("Saving profile details for {0}", request.UserName);
            ApplicationUserDetails entity = _mapping.Map(request);
            var currentRecord = _repository.Find(c => c.UserId == request.UserId).FirstOrDefault();
            if(currentRecord == null)
            {
                var result = await _repository.AddAsync(entity);
            }
            else
            {
                await _repository.UpdateAsync(entity);
            }

            FindByUsernameRequest findByUsernameRequest = new FindByUsernameRequest { Username = request.UserName };
            return await _mediator.Send(findByUsernameRequest, cancellationToken);
        }
    }
}