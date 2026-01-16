using MediatR;
using Standard.Tool.Platform.Data.Entities;
using Standard.Tool.Platform.Data.Infrastructure;
using System.Threading;
using System.Threading.Tasks;

namespace Standard.Tool.Platform.Auth
{
    public record CountAccountsQuery : IRequest<int>;

    public class CountAccountsQueryHandler : IRequestHandler<CountAccountsQuery, int>
    {
        private readonly IRepository<AccountEntity> _repo;

        public CountAccountsQueryHandler(IRepository<AccountEntity> repo) => _repo = repo;

        public Task<int> Handle(CountAccountsQuery request, CancellationToken ct) => _repo.CountAsync(ct: ct);
    }
}