using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using CSharpFunctionalExtensions;
using TipCatDotNet.Api.Models.HospitalityFacilities;

namespace TipCatDotNet.Api.Services.HospitalityFacilities
{
    public interface IStripeAccountService
    {
        Task<Result<string>> Add(int memberId, MemberRequest request, CancellationToken cancellationToken);
        Task<Result> Update(MemberRequest request, CancellationToken cancellationToken);
        Task<Result> Remove(int memberId, CancellationToken cancellationToken);
        // TODO : get cards info; 
    }
}