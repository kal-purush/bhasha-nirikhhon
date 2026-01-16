using MediatR;
using SFA.DAS.EmployerRequestApprenticeTraining.Domain.Interfaces;
using SFA.DAS.EmployerRequestApprenticeTraining.Infrastructure.Api.Responses;

namespace SFA.DAS.EmployerRequestApprenticeTraining.Application.Commands.PostStandard
{
    public class PostStandardCommandHandler : IRequestHandler<PostStandardCommand, Standard>
    {
        private readonly IEmployerRequestApprenticeTrainingOuterApi _outerApi;

        public PostStandardCommandHandler(IEmployerRequestApprenticeTrainingOuterApi outerApi)
        {
            _outerApi = outerApi;
        }

        public async Task<Standard> Handle(PostStandardCommand command, CancellationToken cancellationToken)
        {
            return await _outerApi.PostStandard(command.StandardId);
        }
    }
}