using System.ComponentModel.DataAnnotations;
using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using Newtonsoft.Json;

namespace Soenneker.Dtos.IdNamePair;

/// <summary>
/// A minimal Record type with an <see cref="Id"/> (string), <see cref="Name"/> (string), and maximum JSON compatibility. <para/>
/// The reason for this type (over something like KeyValuePair, dictionary) is because of JSON serialization support.
/// </summary>
/// <remarks>Record type, so avoid modification after initial construction.</remarks>
[DataContract]
public record IdNamePair
{
    [Required]
    [DataMember(Name = "id")]
    [JsonPropertyName("id")]
    [JsonProperty("id")]
    public string Id { get; set; } = default!;

    [Required]
    [DataMember(Name = "name")]
    [JsonPropertyName("name")]
    [JsonProperty("name")]
    public string Name { get; set; } = default!;
}