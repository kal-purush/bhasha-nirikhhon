using System;
using System.Runtime.Serialization;

namespace EA.Serializers.UnitTests.Models;

[DataContract]
public class DummyModel
{
    [DataMember(Order = 1)]
    public Guid Id { get; set; }

    [DataMember(Order = 2)]
    public string Description { get; set; }

    [DataMember(Order = 3)]
    public DateTime CreatedNow { get; set; }

    [DataMember(Order = 4)]
    public DateTime CreatedUtcNow { get; set; }

    [DataMember(Order = 5)]
    public int Count { get; set; }

    [DataMember(Order = 6)]
    public char Initial { get; set; }

    [DataMember(Order = 7)]
    public decimal Amount { get; set; }

    [DataMember(Order = 8)]
    public float Percent { get; set; }

    [DataMember(Order = 9)]
    public double Max { get; set; }

    [DataMember(Order = 10)]
    public DummyModel[] Related { get; set; }

    public static DummyModel Build()
    {
        var item = new DummyModel
        {
            Id = Guid.NewGuid(),
            Description = "dummy model",
            Count = 100,
            CreatedNow = DateTime.Now,
            CreatedUtcNow = DateTime.UtcNow,
            Initial = 'O',
            Amount = 16.99M,
            Percent = 23.14f,
            Max = double.MaxValue,
            Related = new DummyModel[]
            {
                    new DummyModel
                    {
                        Id = Guid.NewGuid(),
                        Description = "testing model self reference",
                        Count = 1000,
                        CreatedNow = DateTime.Now,
                        CreatedUtcNow = DateTime.UtcNow,
                        Initial = 'M',
                        Amount = 456456.456M,
                        Percent = 789.789f,
                        Max = double.MaxValue,
                        Related = null
                    }
            }
        };
        return item;
    }
}
