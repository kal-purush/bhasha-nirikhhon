using Application.Abstractions.Messaging;
using Domain.Entities;
using Domain.Repositories;
using Domain.Shared;

namespace Application.TodoEntries.Queries.GetTodoEntries
{
    public sealed class GetTodoEntriesQueryHandler : IQueryHandler<GetTodoEntriesQuery, Result>
    {
        public readonly IRepository _repository;

        public GetTodoEntriesQueryHandler(IRepository repository) => _repository = repository;

        public async Task<Result> Handle(GetTodoEntriesQuery request, CancellationToken cancellationToken)
        {
            var result = await _repository.GetTodoEntries();

            if (result is null)
            {
                return Result.Failure(TodoEntryError.EmptyOrNull);
            }

            return Result.Success(new GetTodoEntriesResponse(result));
        }
    }
}