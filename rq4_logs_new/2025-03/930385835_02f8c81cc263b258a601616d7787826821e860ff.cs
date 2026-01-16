using PetFamily.Application.Dto.Shared;

namespace PetFamily.Application.Dto.Pet;

public class PetDto
{
    public Guid Id { get; init; }
    public Guid VolunteerId { get; init; }
    public string Name { get; init; } = string.Empty;
    public string Description { get; init; }  = string.Empty;
    public PetClassificationDto PetClassification { get; init; } = new(Guid.Empty, Guid.Empty);
    public string Color { get; init; }  = string.Empty;
    public string HealthInfo { get; init; }  = string.Empty;
    public AddressDto Address { get; init; } = new(string.Empty, string.Empty, string.Empty);
    public float Weight { get; init; }
    public float Height { get; init; }
    public string OwnerPhoneNumber { get; init; }  = string.Empty;
    public bool IsCastrated { get; init; }
    public DateTime DateOfBirth { get; init; }
    public bool IsVaccinated { get; init; }
    public string HelpStatus { get; init; }  = string.Empty;
    public TransferDetailsDto TransferDetails { get; init; } = new([]);
    public IEnumerable<string> Photos { get; init; } = [];
    public DateTime CreationDate { get; init; }
    public int Position { get; init; }
}