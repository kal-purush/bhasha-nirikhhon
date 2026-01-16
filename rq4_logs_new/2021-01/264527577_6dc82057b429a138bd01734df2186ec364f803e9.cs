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

    public class
        GetAllMovimentsQueryHandler : IRequestHandler<GetAllMovimentsQuery, IEnumerable<GetAllMovimentResponse>>
    {
        private readonly IMovimentRepository _movimentRepository;

        public GetAllMovimentsQueryHandler(IMovimentRepository movimentRepository)
        {
            _movimentRepository = movimentRepository;
        }

        public async Task<IEnumerable<GetAllMovimentResponse>> Handle(GetAllMovimentsQuery request,
            CancellationToken cancellationToken)
        {
            var moviments = await _movimentRepository.GetMovimentsByAccount(request.UserId,
                request.AccountId, request.Page, request.PageSize);

            if (moviments is null || !moviments.Any())
                return Enumerable.Empty<GetAllMovimentResponse>();

            return moviments.Select(m => new GetAllMovimentResponse
            {
                Id = m.Id,
                Value = m.Value,
                Description = m.Description,
                Category = m.Category,
                Type = m.Type,
                CreatedOn = m.CreatedOn
            });
        }
    }
}