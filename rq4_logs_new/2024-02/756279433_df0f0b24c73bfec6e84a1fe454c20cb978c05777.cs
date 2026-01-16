using AskHand.Domain.Addresses;
using AskHand.Domain.Levels;

namespace AskHand.Domain.Walls;

public sealed class Wall : AggregateRoot<WallId>
{
    public Level Level { get; }

    public bool IsIndoor { get; }

    public bool IsBlock { get; }

    public bool IsPresent { get; }

    public Address Address { get; }

    private Wall(
        WallId id,
        Level level,
        bool isIndoor,
        bool isBlock,
        bool isPresent, 
        Address address)
        : base(id)
    {
        Level = level;
        IsIndoor = isIndoor;
        IsBlock = isBlock;
        IsPresent = isPresent;
        Address = address;
    }

    public static Wall Create(
        Level level,
        bool isIndoor,
        bool isBlock,
        bool isPresent,
        Address address)
    {
        return new Wall(
            WallId.CreateUnique(),
            Level.Create(level.Rate), 
            isIndoor,
            isBlock,
            isPresent,
            Address.Create(address.Number, address.Street, address.ZipCode, address.City, address.Country)); 
    }
}