using DAL.Data;
using DAL.Models;
using MediatR;

namespace BLL.Handlers.GameHistories
{
    public class CreateGameHistory
    {
        public class Command : IRequest
        {
            public GameHistory GameHistory { get; set; }
        }

        public class Handler : IRequestHandler<Command>
        {
            private readonly DataContext _context;

            public Handler(DataContext context)
            {
                _context = context;
            }

            public async Task<Unit> Handle(Command request, CancellationToken cancellationToken)
            {
                await _context.GameHistories.AddAsync(request.GameHistory);

                await _context.SaveChangesAsync();

                return Unit.Value;
            }
        }
    }
}