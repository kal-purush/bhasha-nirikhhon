using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Application.Core;
using Application.Interfaces;
using AutoMapper;
using Domain;
using MediatR;
using Microsoft.EntityFrameworkCore;
using Persistence;

namespace Application.Jobs
{
    public class Add
    {
        public class Command : IRequest<Result<Unit>>
        {
            public Guid Id { get; set; }
            public string Title { get; set; }
            public string Description { get; set; }
            public ICollection<UserFile> Attachments { get; set; }
            public bool IsActive { get; set; }
            public ICollection<Skill> RequiredSkills { get; set; }
        }

        public class Handler : IRequestHandler<Command, Result<Unit>>
        {
            private readonly DataContext _context;
            private readonly IMapper _mapper;
            private readonly IUserAccessor _userAccessor;
            public Handler(DataContext context, IMapper mapper, IUserAccessor userAccessor)
            {
                _userAccessor = userAccessor;
                _mapper = mapper;
                _context = context;
            }

            public async Task<Result<Unit>> Handle(Command request, CancellationToken cancellationToken)
            {
                var user = await _context.Users
                .FirstOrDefaultAsync(x => x.UserName == _userAccessor.GetUsername());

                if (user == null) return null;

                var skills = new List<Skill>();

                foreach (Skill reqSkill in request.RequiredSkills)
                {
                    var skill = await _context.Skills.FirstOrDefaultAsync(s => s.Id == reqSkill.Id);
                    skills.Add(skill);
                }


                var job = new Job
                {
                    Id = request.Id,
                    Title = request.Title,
                    Description = request.Description,
                    Attachments = request.Attachments,
                    IsActive = request.IsActive,
                    CreationTime = DateTime.UtcNow,
                    RequiredSkills = skills,
                    Employer = user
                };

                _context.Jobs.Add(job);

                var result = await _context.SaveChangesAsync() > 0;

                if (result) return Result<Unit>.Success(Unit.Value);

                return Result<Unit>.Failure("Problem adding job");

            }
        }
    }
}