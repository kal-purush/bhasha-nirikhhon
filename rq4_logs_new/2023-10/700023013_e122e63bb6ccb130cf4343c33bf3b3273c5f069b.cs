using Application.Commons;
using Application.Users.Entities;
using AutoMapper;
using MediatR;
using Microsoft.EntityFrameworkCore;

namespace Application.Users.Features.UpdateUser;

public class UpdateUserHandler : IRequestHandler<UpdateUserRequest, bool>
{
    private readonly IMapper _mapper;
    private readonly AppDbContext _ctx;

    public UpdateUserHandler(AppDbContext ctx, IMapper mapper)
    {
        _ctx = ctx;
        _mapper = mapper;
    }

    public async Task<bool> Handle(UpdateUserRequest request, CancellationToken cancellationToken)
    {
        var user = await _ctx.GetEntity<User>()
            .Include(x=>x.UserDetails)
            .Include(x=>x.UserEmails)
            .Include(x=>x.UserPhones)
            .FirstOrDefaultAsync(x=>x.Id == request.Id);

        user.Name = request.Name;
        user.Surname = request.Surname;
        user.Birthdate = request.Birthdate;
        user.IdRole = request.IdRole;
        user.IdCitizenship = request.IdCitizenship;
        user.EditDate = DateTime.UtcNow;

        user.UserDetails.IdUserType = request.IdUserType;
        user.UserDetails.HasChild = request.HasChild;
        user.UserDetails.IsSmoker = request.IsSmoker;
        user.UserDetails.HasBankStatement = request.HasBankStatement;
        user.UserDetails.HasUmowaOkazionalny = request.HasUmowaOkazionalny;
        user.UserDetails.HasWorkContract = request.HasWorkContract;
        user.UserDetails.HasWorkPermit = request.HasWorkPermit;
        user.EditDate = DateTime.UtcNow;
        
        if (!string.IsNullOrEmpty(request.PhoneNumber))
        {
            user.UserPhones = new List<UserPhone>()
            {
                new ()
                {
                    IdCountryCode = request.PhoneCountryCode,
                    PhoneNumber = request.PhoneNumber,
                    CreateDate = DateTime.UtcNow
                }
            };
        }
        
        if (!string.IsNullOrEmpty(request.Email))
        {
            user.UserEmails = new List<UserEmail>()
            {
                new ()
                {
                    Email = request.Email,
                    CreateDate = DateTime.UtcNow
                }
            };
        }
        
        _ctx.GetEntity<User>().Update(user);

        var affectedRows = await _ctx.SaveChangesAsync(cancellationToken);

        return affectedRows >= 1;
    }
}