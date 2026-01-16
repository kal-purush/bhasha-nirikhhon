using MediatR;
using Microsoft.EntityFrameworkCore;
using NpgsqlTypes;
using retrowebcore.Persistences;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace retrowebcore.Handlers.Boards
{
    public class SearchBoardRequest : IRequest<BoardListResponse>
    {
        public int Offset { get; set; } = 0;
        public int Limit { get; set; } = 100;

        public string Query { get; set; }
        public SearchBoardRequest(string q) => Query = q;
    }

    public class BoardSearchHandler : BoardHandlerBase, IRequestHandler<SearchBoardRequest, BoardListResponse>
    {
        static readonly string BoardSearchHandlerQuery = nameof(BoardSearchHandlerQuery);
        public BoardSearchHandler(AppDbContext c) : base(c) { }
        public async Task<BoardListResponse> Handle(SearchBoardRequest r, CancellationToken ct)
        {
            var splitted = r.Query.Split(' ');
            var query = string.Join('&', splitted.Where(x => !string.IsNullOrWhiteSpace(x)));
            var data = await _context.Boards
                .TagWith(BoardSearchHandlerQuery)
                .Where(x => x.Deletor == null && x.DeletedAt == null)
                .Where(x => EF.Property<NpgsqlTsVector>(x, AppDbContext.SearchablePropertyVector)
                            .Matches(EF.Functions.ToTsQuery(query)))
                .OrderBy(x => x.Created)
                .Skip(r.Offset)
                .Take(r.Limit)
                .ToListAsync();
            var hasMore = false;
            var hasPrev = false;
            if (data.Count > 0)
            {
                var soonest = data.First();
                var oldest = data.Last();
                hasMore = await _context.Boards
                    .OrderBy(x => x.Created)
                    .AnyAsync(x =>
                        x.DeletedAt == null &&
                        x.Deletor == null &&
                        EF.Property<NpgsqlTsVector>(x, AppDbContext.SearchablePropertyVector)
                            .Matches(EF.Functions.ToTsQuery(query)) &&
                        x.Created > oldest.Created &&
                        x.Id != oldest.Id);
                hasPrev = await _context.Boards
                    .OrderBy(x => x.Created)
                    .Skip(r.Offset)
                    .AnyAsync(x =>
                        x.DeletedAt == null &&
                        x.Deletor == null &&
                        EF.Property<NpgsqlTsVector>(x, AppDbContext.SearchablePropertyVector)
                            .Matches(EF.Functions.ToTsQuery(query)) &&
                        x.Created < soonest.Created &&
                        x.Id != soonest.Id);
            }

            return new BoardListResponse { HasPrev = hasPrev, HasNext = hasMore, Data = data };
        }
    }
}