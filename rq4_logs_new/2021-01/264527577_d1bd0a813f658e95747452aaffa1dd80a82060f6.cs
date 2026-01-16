namespace FS.Api.Application.Queries.Handlers
{
    using DataObject.Moviment.Response;
    using Domain.Core.Interfaces;
    using MediatR;
    using Query;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    public class GetAllInvestmentsQueryHandler : IRequestHandler<GetAllInvestmentsQuery, IEnumerable<GetAllInvestmentResponse>>
    {
        private readonly IInvestmentRepository _investmentRepository;

        public GetAllInvestmentsQueryHandler(IInvestmentRepository investmentRepository)
        {
            _investmentRepository = investmentRepository;
        }

        public async Task<IEnumerable<GetAllInvestmentResponse>> Handle(GetAllInvestmentsQuery request,
            CancellationToken cancellationToken)
        {
            var investments = await _investmentRepository.GetInvestmentsByAccount(request.UserId,
                request.AccountId, request.Page, request.PageSize);

            if (investments is null || !investments.Any())
                return Enumerable.Empty<GetAllInvestmentResponse>();

            return investments.Select(m => new GetAllInvestmentResponse
            {
                Id = m.Id,
                Value = m.Value,
                Description = m.Description,
                Type = m.Type,
                CreatedOn = m.CreatedOn
            });
        }
    }
}