using Marten;

namespace SimpleWebApi8;

public static class ThingHandlers
{
    public static async Task<MyThing?> CreateMyThing(DocumentStore store)
    {
        var thingId = Guid.NewGuid();

        await using var session = store.LightweightSession();
        var myThingInitialized = new MyThingInitialized
        {
            Name = "My Thing",
            Description = "This is my thing",
            Tags = ["tag1", "tag2"],
            Picture = [], // replace with your picture bytes
            Place = "My place"
        };

        session.Events.StartStream<MyThing>(thingId, myThingInitialized);
        await session.SaveChangesAsync();

        return session.Events.AggregateStream<MyThing>(thingId);
    }

    public static async Task<MyThing?> GetMyThingById(Guid myThingId, DocumentStore store)
    {
        await using var session = store.LightweightSession();
        return session.Events.AggregateStream<MyThing>(myThingId);
    }
}