using Sample.Domain.Interfaces;
using Sample.Domain.Interfaces.Queries;
using Sample.Domain.Models;

namespace Sample.Application.Queries.Handlers
{
    public class GetUserByIdQueryHandler(IRepository<User> repository) : IQueryHandler<GetUserByIdQuery, User>
    {
        private readonly IRepository<User> _repository = repository;

        public async Task<Response<User>> Handle(GetUserByIdQuery query)
        {
            User user = await _repository.FindByIdAsync(query.Id);
            return new() { Data = user, Message = "", Success = true };
        }
    }
}