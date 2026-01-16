using System.Text.Json;
using System.Text.Json.Serialization;

namespace Sportradar.Mbs.Sdk.Entities.Payout;

[JsonConverter(typeof(PayoutSourceTypeJsonConverter))]
public enum PayoutSourceType
{
  EXT_SETTLEMENT,
  FREE_ROLLOVER,
  CASHOUT,
  CASH,
  MANUAL_CASHOUT,
  MANUAL_CANCEL,
  BONUS,
  CANCEL,
  FREE,
  ODDS_BOOST,
  FREE_CASH
}

public class PayoutSourceTypeJsonConverter : JsonConverter<PayoutSourceType>
{

  public override PayoutSourceType Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
  {
    var jsonVal = reader.GetString();
    return jsonVal switch
    {
      "ext-settlement" => PayoutSourceType.EXT_SETTLEMENT,
      "free-rollover" => PayoutSourceType.FREE_ROLLOVER,
      "cashout" => PayoutSourceType.CASHOUT,
      "cash" => PayoutSourceType.CASH,
      "manual-cancel" => PayoutSourceType.MANUAL_CASHOUT,
      "manual-cashout" => PayoutSourceType.MANUAL_CANCEL,
      "bonus" => PayoutSourceType.BONUS,
      "cancel" => PayoutSourceType.CANCEL,
      "free" => PayoutSourceType.FREE,
      "odds-boost" => PayoutSourceType.ODDS_BOOST,
      "free-cash" => PayoutSourceType.FREE_CASH,
      _ => throw new JsonException("Unknown type of PayoutSourceType: " + jsonVal)
    };
  }

  public override void Write(Utf8JsonWriter writer, PayoutSourceType value, JsonSerializerOptions options)
  {
    var jsonVal = value switch
    {
      PayoutSourceType.EXT_SETTLEMENT => "ext-settlement",
      PayoutSourceType.FREE_ROLLOVER => "free-rollover",
      PayoutSourceType.CASHOUT => "cashout",
      PayoutSourceType.CASH => "cash",
      PayoutSourceType.MANUAL_CASHOUT => "manual-cancel",
      PayoutSourceType.MANUAL_CANCEL => "manual-cashout",
      PayoutSourceType.BONUS => "bonus",
      PayoutSourceType.CANCEL => "cancel",
      PayoutSourceType.FREE => "free",
      PayoutSourceType.ODDS_BOOST => "odds-boost",
      PayoutSourceType.FREE_CASH => "free-cash",
      _ => throw new ArgumentOutOfRangeException(nameof(value), value, null)
    };
    writer.WriteStringValue(jsonVal);
  }
}