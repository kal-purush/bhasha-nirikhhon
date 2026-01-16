namespace ApiaryManagementSystem.Application.Features.Diseases.Commands.DeleteDisease;

using System.Threading;
using System.Threading.Tasks;
using ApiaryManagementSystem.Application.Common.Interfaces;
using Ardalis.GuardClauses;
using MediatR;
using Microsoft.EntityFrameworkCore;

public sealed class DeleteDiseaseCommand : IRequest
{
    public Guid DiseaseId { get; init; }
}

internal sealed class DeleteDiseaseCommandHandler(IApplicationDbContext dbContext) : IRequestHandler<DeleteDiseaseCommand>
{
    public async Task Handle(DeleteDiseaseCommand request, CancellationToken cancellationToken)
    {
        var disease = await dbContext.Diseases
            .Where(x => x.Id == request.DiseaseId)
            .FirstOrDefaultAsync(cancellationToken);

        Guard.Against.NotFound(request.DiseaseId, disease);

        dbContext.Diseases.Remove(disease);

        await dbContext.SaveChangesAsync(cancellationToken);
    }
}