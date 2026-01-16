using Application.Common.Interfaces;
using Domain.Entities;
using Domain.Enums;
using MediatR;
using Microsoft.Extensions.Logging;
using System.Text.Json.Serialization;

namespace Application.TodoItems.Commond.CreateTodoItem
{
    public record CreateTodoItemCommand : IRequest<string>
    {
        public string? ListId { get; init; }
        public string? Title { get; init; }
        public string? Notes { get; init; }
        public Priority Priority { get; init; }
    }

    public class CreateTodoItemCommandHandler : IRequestHandler<CreateTodoItemCommand, string>
    {
        private readonly ILogger<CreateTodoItemCommandHandler> _logger;
        private readonly IAppDbContext _context;
        private readonly IDateTime _dateTime;
        private readonly ICurrentUserService _currentUserService;

        public CreateTodoItemCommandHandler(ILogger<CreateTodoItemCommandHandler> logger, IAppDbContext context, IDateTime dateTime, ICurrentUserService currentUserService)
        {
            _logger = logger;
            _context = context;
            _dateTime = dateTime;
            _currentUserService = currentUserService;
        }

        public async Task<string> Handle(CreateTodoItemCommand request, CancellationToken cancellationToken)
        {
            try
            {
                var item = new TodoItem
                {
                    CreatedAt = _dateTime.Now,
                    CreatedBy = _currentUserService.UserId,
                    ListId = request.ListId,
                    Title = request.Title,
                    Notes = request.Notes,
                    Priority = request.Priority,
                };
                await _context.TodoItems.AddAsync(item, cancellationToken);
                await _context.SaveChangesAsync(cancellationToken);
                return item.Id;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
                throw;
            }
        }
    }
}