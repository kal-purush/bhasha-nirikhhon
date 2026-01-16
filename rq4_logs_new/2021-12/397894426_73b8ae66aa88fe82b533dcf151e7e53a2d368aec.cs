using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Application.Core;
using Application.Interfaces;
using Domain;
using MediatR;
using Microsoft.EntityFrameworkCore;
using Persistence;

namespace Application.Jobs
{
    public class AddBid
    {
        public class Command : IRequest<Result<Unit>>
        {
            public Guid JobId { get; set; }
            public string Description { get; set; }
            public double Fee { get; set; }
            public UserFile CV { get; set; }
        }

        public class Handler : IRequestHandler<Command, Result<Unit>>
        {
            private readonly DataContext _context;
            private readonly IUserAccessor _userAccessor;
            private readonly IFileAccessor _fileAccessor;
            public Handler(DataContext context, IUserAccessor userAccessor, IFileAccessor fileAccessor)
            {
                _fileAccessor = fileAccessor;
                _userAccessor = userAccessor;
                _context = context;
            }

            public async Task<Result<Unit>> Handle(Command request, CancellationToken cancellationToken)
            {
                var user = await _context.Users
                .FirstOrDefaultAsync(u => u.UserName == _userAccessor.GetUsername());

                if (user == null) return null;

                var job = await _context.Jobs
                .Include(j => j.JobBids)
                .FirstOrDefaultAsync(j => j.Id == request.JobId);

                if (job == null) return null;

                Console.WriteLine(request.Description);

                var jobBid = new JobBid
                {
                    Bidder = user,
                    Job = job,
                    Description = request.Description,
                    Fee = request.Fee,
                };

                job.JobBids.Add(jobBid);

                var result = await _context.SaveChangesAsync() > 0;

                if (result) return Result<Unit>.Success(Unit.Value);

                return Result<Unit>.Failure("Problem adding job");
            }
        }
    }
}