using MediatR;
using SFA.DAS.EmployerRequestApprenticeTraining.Domain.Interfaces;
using SFA.DAS.EmployerRequestApprenticeTraining.Infrastructure.Api.Requests;

namespace SFA.DAS.EmployerRequestApprenticeTraining.Application.Commands.CancelEmployerRequest
{
    public class CancelEmployerRequestCommandHandler : IRequestHandler<CancelEmployerRequestCommand>
    {
        private readonly IEmployerRequestApprenticeTrainingOuterApi _outerApi;

        public CancelEmployerRequestCommandHandler(IEmployerRequestApprenticeTrainingOuterApi outerApi)
        {
            _outerApi = outerApi;
        }

        public async Task Handle(CancelEmployerRequestCommand command, CancellationToken cancellationToken)
        {
            await _outerApi.CancelEmployerRequest(command.EmployerRequestId, 
                new CancelEmployerRequestRequest 
                { 
                    CancelledBy = command.CancelledBy, 
                    DashboardUrl = command.DashboardUrl 
                });
        }
    }
}