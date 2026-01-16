namespace ApiaryManagementSystem.Application.Features.Diseases.Commands.UpdateDisease;

using System.Threading;
using System.Threading.Tasks;
using ApiaryManagementSystem.Application.Common.Interfaces;
using ApiaryManagementSystem.Domain.Events.Diseases;
using Ardalis.GuardClauses;
using MediatR;

public sealed class UpdateDiseaseCommand : IRequest
{
    public Guid Id { get; init; }

    public required string Name { get; init; }

    public required string Treatment { get; init; }

    public string? Description { get; init; }

    public Guid HiveId { get; init; }
}

internal sealed class UpdateDiseaseCommandHandler(IApplicationDbContext dbContext) : IRequestHandler<UpdateDiseaseCommand>
{
    public async Task Handle(UpdateDiseaseCommand request, CancellationToken cancellationToken)
    {
        var disease = await dbContext.Diseases
            .FindAsync([request.Id], cancellationToken);

        Guard.Against.NotFound(request.Id, disease);

        disease.UpdateDisease(
            request.Name,
            request.Treatment,
            request.Description,
            request.HiveId);

        disease.AddDomainEvent(new DiseaseUpdatedEvent());

        await dbContext.SaveChangesAsync(cancellationToken);
    }
}