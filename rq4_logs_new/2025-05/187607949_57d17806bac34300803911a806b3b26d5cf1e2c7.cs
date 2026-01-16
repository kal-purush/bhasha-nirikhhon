using System.Text.Json.Serialization;

namespace SFA.DAS.Recruit.Api.Domain.Models;

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum OwnerType
{
    Employer = 0,
    Provider = 1,
    External = 2,
    Unknown = 3
}