using CSharpFunctionalExtensions;
using FluentValidation;
using P2Project.Core.Dtos.VolunteerRequests;
using P2Project.Core.Extensions;
using P2Project.Core.Interfaces.Queries;
using P2Project.Core.Models;
using P2Project.SharedKernel.Errors;

namespace P2Project.VolunteerRequests.Application.VolunteerRequestsManagement.Queries.GetAllByUserId;

public class GetAllByUserIdHandler :
    IQueryValidationHandler<PagedList<VolunteerRequestDto>, 
        GetAllByUserIdQuery>
{
    private readonly IValidator<GetAllByUserIdQuery> _validator;
    private readonly IVolunteerRequestsReadDbContext _readDbContext;

    public GetAllByUserIdHandler(
        IValidator<GetAllByUserIdQuery> validator,
        IVolunteerRequestsReadDbContext readDbContext)
    {
        _validator = validator;
        _readDbContext = readDbContext;
    }

    public async Task<Result<PagedList<VolunteerRequestDto>, ErrorList>> Handle(
        GetAllByUserIdQuery query,
        CancellationToken cancellationToken)
    {
        var validationResult = await _validator.ValidateAsync(query, cancellationToken);
        if (validationResult.IsValid == false)
            return validationResult.ToErrorList();
        
        var requestsQuery = _readDbContext.VolunteerRequests.WhereIf(true,
            x => x.Status == query.Status);
        
        requestsQuery = requestsQuery.WhereIf(
            !string.IsNullOrWhiteSpace(
                query.UserId.ToString()), x => x.UserId == query.UserId);
        
        requestsQuery = requestsQuery.WhereIf(
            !string.IsNullOrWhiteSpace(
                query.Status), x => x.Status == query.Status);
        
        return await requestsQuery
            .ToPagedList(query.Page, query.PageSize, cancellationToken);
    }
}