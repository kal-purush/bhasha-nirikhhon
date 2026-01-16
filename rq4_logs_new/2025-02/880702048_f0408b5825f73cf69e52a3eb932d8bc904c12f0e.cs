using CSharpFunctionalExtensions;
using FluentValidation;
using P2Project.Core.Dtos.VolunteerRequests;
using P2Project.Core.Extensions;
using P2Project.Core.Interfaces.Queries;
using P2Project.Core.Models;
using P2Project.SharedKernel.Errors;
using P2Project.VolunteerRequests.Domain.Enums;

namespace P2Project.VolunteerRequests.Application.VolunteerRequestsManagement.Queries.GetAllByAdminId;

public class GetAllByAdminIdHandler :
    IQueryValidationHandler<PagedList<VolunteerRequestDto>, 
        GetAllByAdminIdQuery>
{
    private readonly IValidator<GetAllByAdminIdQuery> _validator;
    private readonly IVolunteerRequestsReadDbContext _readDbContext;

    public GetAllByAdminIdHandler(
        IValidator<GetAllByAdminIdQuery> validator,
        IVolunteerRequestsReadDbContext readDbContext)
    {
        _validator = validator;
        _readDbContext = readDbContext;
    }

    public async Task<Result<PagedList<VolunteerRequestDto>, ErrorList>> Handle(
        GetAllByAdminIdQuery query,
        CancellationToken cancellationToken)
    {
        var validationResult = await _validator.ValidateAsync(query, cancellationToken);
        if (validationResult.IsValid == false)
            return validationResult.ToErrorList();
        
        var requestsQuery = _readDbContext.VolunteerRequests.WhereIf(true,
            x => x.Status == query.Status);
        
        requestsQuery = requestsQuery.WhereIf(
            !string.IsNullOrWhiteSpace(
                query.AdminId.ToString()), x => x.AdminId == query.AdminId);
        
        requestsQuery = requestsQuery.WhereIf(
            string.IsNullOrWhiteSpace(
                query.AdminId.ToString()), x => x.Status == RequestStatus.OnReview.ToString());
        
        requestsQuery = requestsQuery.WhereIf(
            !string.IsNullOrWhiteSpace(
                query.AdminId.ToString()), x => x.Status == query.Status);
        
        return await requestsQuery
            .ToPagedList(query.Page, query.PageSize, cancellationToken);
    }
}