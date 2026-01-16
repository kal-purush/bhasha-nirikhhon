namespace ApiaryManagementSystem.Application.Features.Diseases.Commands.CreateDisease;

using System.Threading;
using System.Threading.Tasks;
using ApiaryManagementSystem.Application.Common.Interfaces;
using ApiaryManagementSystem.Domain.Events.Diseases;
using ApiaryManagementSystem.Domain.Models.Diseases;
using MediatR;

public sealed class CreateDiseaseCommand : IRequest<Guid>
{
    public required string Name { get; init; }

    public required string Treatment { get; init; }

    public string? Description { get; init; }

    public Guid HiveId { get; init; }
}

internal sealed class CreateDiseaseCommandHandler(IApplicationDbContext dbContext) : IRequestHandler<CreateDiseaseCommand, Guid>
{
    public async Task<Guid> Handle(CreateDiseaseCommand request, CancellationToken cancellationToken)
    {
        var disease = new Disease(
            request.Name,
            request.Treatment,
            request.Description,
            request.HiveId);

        dbContext.Diseases.Add(disease);

        disease.AddDomainEvent(new DiseaseCreatedEvent());

        await dbContext.SaveChangesAsync(cancellationToken);

        return disease.Id;
    }
}