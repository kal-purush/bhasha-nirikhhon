using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace PetFamily.Infrastructure.Extensions;

public static class EfCorePropertyExtensions
{ 
    public static PropertyBuilder<IReadOnlyList<TValueObject>> Json1DeepLvlVoCollectionConverter<TValueObject, TDto>(
        this PropertyBuilder<IReadOnlyList<TValueObject>> builder, Func<TValueObject, TDto> selector,
        Func<TDto, TValueObject> reverseSelector)
    {
        return builder.HasConversion(
                vo => SerializeVoCollection(vo, selector),
                json => DeserializeDtoCollection(json, reverseSelector),
                new ValueComparer<IReadOnlyList<TValueObject>>(
                    (c1, c2) => c1!.SequenceEqual(c2!),
                    c => c.Aggregate(0, (a, v) => HashCode.Combine(a, v!.GetHashCode())),
                    c => c.ToList()))
            .HasColumnType("jsonb");
    }

    private static string SerializeVoCollection<TValueObject, TDto>(
        IReadOnlyList<TValueObject> valueObjects,
        Func<TValueObject, TDto> selector)
    {
        var dtos = valueObjects.Select(selector);
        
        return JsonSerializer.Serialize(dtos, JsonSerializerOptions.Default);
    }

    private static IReadOnlyList<TValueObject> DeserializeDtoCollection<TValueObject, TDto>(
        string json, Func<TDto, TValueObject> selector)
    {
        var dtos = JsonSerializer.Deserialize<IEnumerable<TDto>>(json, JsonSerializerOptions.Default) ?? [];

        return dtos.Select(selector).ToList();
    }
}   