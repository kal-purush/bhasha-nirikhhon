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
    public class Delete
    {
        public class Command : IRequest<Result<Unit>>
        {
            public Guid Id { get; set; }
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
                .Include(u => u.PostedJobs).ThenInclude(j => j.Attachments)
                .FirstOrDefaultAsync(u => u.UserName == _userAccessor.GetUsername());

                if (user == null) return null;

                var job = user.PostedJobs.FirstOrDefault(j => j.Id == request.Id);

                if (job == null) return null;

                if (job.Attachments.Count > 0)
                {
                    var fileIds = new List<string>();

                    foreach (var attachment in job.Attachments)
                    {
                        fileIds.Add(attachment.Id);
                    }

                    var result = await _fileAccessor.DeleteFiles(fileIds);

                    if (result == null) return Result<Unit>.Failure("Problem deleting files from Cloudinary");
                }

                user.PostedJobs.Remove(job);

                var success = await _context.SaveChangesAsync() > 0;

                if (success) return Result<Unit>.Success(Unit.Value);

                return Result<Unit>.Failure("Problem deleting job.");
            }
        }
    }
}