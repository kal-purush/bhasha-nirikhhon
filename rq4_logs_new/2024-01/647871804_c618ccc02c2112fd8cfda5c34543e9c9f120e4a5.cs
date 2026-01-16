using MeowLib.Domain.Character.Enums;

namespace MeowLib.WebApi.Models.Requests.v1.Book;

public class AttachCharacterRequest
{
    public required int CharacterId { get; init; }
    public required BookCharacterRoleEnum Role { get; init; }
}