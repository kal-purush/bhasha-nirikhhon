using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Application.Core;
using Application.Interfaces;
using MediatR;
using Microsoft.EntityFrameworkCore;
using Persistence;

namespace Application.Jobs
{
    public class AcceptBid
    {
        public class Command : IRequest<Result<Unit>>
        {
            public Guid JobId { get; set; }
            public Guid JobBidId { get; set; }
        }

        public class Handler : IRequestHandler<Command, Result<Unit>>
        {
            private readonly DataContext _context;
            private readonly IUserAccessor _userAccessor;

            public Handler(DataContext context, IUserAccessor userAccessor)
            {
                _context = context;
                _userAccessor = userAccessor;
            }

            public async Task<Result<Unit>> Handle(Command request, CancellationToken cancellationToken)
            {
                var user = await _context.Users.FirstOrDefaultAsync(u => u.UserName == _userAccessor.GetUsername());

                if (user == null) return null;

                var job = await _context.Jobs
                .Include(j => j.JobBids)
                .FirstOrDefaultAsync(j => j.Id == request.JobId);

                if (job == null) return null;

                var jobBid = job.JobBids.FirstOrDefault(jb => jb.Id == request.JobBidId);

                job.AcceptedJobBid = jobBid;

                _context.Entry(job).State = EntityState.Modified;

                _context.JobBids.RemoveRange(job.JobBids.Where(jb => jb.Id != jobBid.Id));

                var success = await _context.SaveChangesAsync() > 0;

                if (success) return Result<Unit>.Success(Unit.Value);

                return Result<Unit>.Failure("Problem updating job");
            }
        }
    }
}