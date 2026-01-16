namespace ApiaryManagementSystem.Application.Features.Diseases.Queries.GetDiseaseById;

using System.Threading;
using System.Threading.Tasks;
using ApiaryManagementSystem.Application.Common.Interfaces;
using Ardalis.GuardClauses;
using AutoMapper;
using AutoMapper.QueryableExtensions;
using MediatR;
using Microsoft.EntityFrameworkCore;

public sealed class GetDiseaseByIdQuery : IRequest<DiseaseModel>
{
    public Guid DiseaseId { get; init; }
}

internal sealed class GetDiseaseByIdQueryHandler(
    IApplicationDbContext dbContext,
    IMapper mapper) : IRequestHandler<GetDiseaseByIdQuery, DiseaseModel>
{
    public async Task<DiseaseModel> Handle(GetDiseaseByIdQuery request, CancellationToken cancellationToken)
    {
        var disease = await dbContext.Diseases
            .Where(x => x.Id == request.DiseaseId)
            .AsNoTracking()
            .ProjectTo<DiseaseModel>(mapper.ConfigurationProvider)
            .FirstOrDefaultAsync(cancellationToken);

        Guard.Against.NotFound(request.DiseaseId, disease);

        return disease;
    }
}