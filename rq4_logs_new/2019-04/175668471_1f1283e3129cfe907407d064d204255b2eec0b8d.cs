using MediatR;
using Microsoft.EntityFrameworkCore;
using RequestService.Application.Queries.Requests.GetRequest;
using RequestService.Domain.Requests;
using RequestService.Infrastructure.Persistence;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace RequestService.Application.Queries.Requests.GetAnswersByRequestId
{
    public class GetAnswersByRequestIdQueryHandler : IRequestHandler<GetAnswersByRequestIdQuery, ICollection<Answer>>
    {
        private readonly RequestServiceDbContext _context;

        public GetAnswersByRequestIdQueryHandler(RequestServiceDbContext context)
        {
            _context = context;
        }

        public async Task<ICollection<Answer>> Handle(GetAnswersByRequestIdQuery request, CancellationToken cancellationToken)
        {
            var entity = await _context.Requests.Include(a => a.Answers).FirstOrDefaultAsync(r => r.Id == request.Id);

            if (entity == null)
                return null;

            return entity.Answers;
        }
    }
}