#nullable disable

using System.Text.Json.Serialization;

namespace Model;

public class Customer
{
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    [JsonPropertyName("id")]
    public string Id { get; set; }

    [JsonPropertyName("fullName")]
    public string FullName { get; set; }

    [JsonPropertyName("birthday")]
    public string Birthday { get; set; }

    [JsonPropertyName("documentId")]
    public string DocumentId { get; set; }

    [JsonPropertyName("email")]
    public string Email { get; set; }

    [JsonPropertyName("postalCode")]
    public string PostalCode { get; set; }

    [JsonPropertyName("cellphone")]
    public string Cellphone { get; set; }

    [JsonPropertyName("consultationDate")]
    public string ConsultationDate { get; set; }

    [JsonPropertyName("consultationType")]
    public string ConsultationType { get; set; }

    [JsonPropertyName("consultationModality")]
    public string ConsultationModality { get; set; }

    [JsonPropertyName("consultationValue")]
    public decimal ConsultationValue { get; set; }

    [JsonPropertyName("isSent")]
    public string IsSent { get; set; }

    [JsonPropertyName("effectiveDate")]
    public string EffectiveDate { get; set; }
}