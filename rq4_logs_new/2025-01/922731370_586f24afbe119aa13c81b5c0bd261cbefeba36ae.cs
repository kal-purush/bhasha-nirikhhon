using Sample.Domain.Interfaces;
using Sample.Domain.Interfaces.Queries;
using Sample.Domain.Models;

namespace Sample.Application.Queries.Handlers
{
	public class GetUsersQueryHandler(IRepository<User> repository) : IQueryHandler<GetUsersQuery, List<User>>
	{
		private readonly IRepository<User> _repository = repository;
		public async Task<Response<List<User>>> Handle(GetUsersQuery query)
		{
			var users = await _repository.ReadAllAsync();
			return new() { Data = users, Message = "Esto esta funcionando una belleza", Success = true };
		}
	}
}