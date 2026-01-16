using System.Text.Json;
using EST.MIT.InvoiceImporter.Function.Models;

namespace EST.MIT.InvoiceImporter.Function.Test.Models;

public class EventActionTests
{
    [Fact]
    public void SerializeAndDeserialize_ReturnsExpectedValues()
    {
        var originalAction = new EventAction
        {
            Type = "Type1",
            Message = "Message1",
            Timestamp = DateTime.UtcNow,
            Data = "Data1"
        };

        var serializedAction = JsonSerializer.Serialize(originalAction);
        var deserializedAction = JsonSerializer.Deserialize<EventAction>(serializedAction);

        Assert.NotNull(deserializedAction);
        Assert.Equal(originalAction.Type, deserializedAction!.Type);
        Assert.Equal(originalAction.Message, deserializedAction.Message);
        Assert.Equal(originalAction.Timestamp, deserializedAction.Timestamp);
        Assert.Equal(originalAction.Data, deserializedAction.Data);
    }
}