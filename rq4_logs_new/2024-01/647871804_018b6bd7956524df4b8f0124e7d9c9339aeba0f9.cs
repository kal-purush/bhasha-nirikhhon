using MeowLib.WebApi.Models.Responses.v1.Tag;
using MeowLib.WebApi.Models.Responses.v1.Translation;

namespace MeowLib.WebApi.Models.Responses.v2.Book;

public class GetBookResponse
{
    public required int Id { get; init; }
    public required string Name { get; init; }
    public required string Description { get; init; }
    public required string? ImageUrl { get; init; }
    public required IEnumerable<PeopleWithBookRoleModel> Peoples { get; init; }
    public required IEnumerable<TagModel> Tags { get; init; }
    public required IEnumerable<TranslationModel> Translations { get; init; }
}