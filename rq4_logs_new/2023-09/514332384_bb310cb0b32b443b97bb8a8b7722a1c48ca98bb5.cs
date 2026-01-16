using ConsoleWorkerFullTest.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleWorkerFullTest.Dto;

public record ProfileDto(Guid Id, string Email)
{
    public static ProfileDto Create(Guid Id, string Email)
        => new (Id, Email);

    public static ProfileDto FromEntity(Profile profile)
        => new (profile.Id, profile.Email);
}