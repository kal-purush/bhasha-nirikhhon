namespace TauCode.Infrastructure.Logging;

public readonly struct ObjectTag : IEquatable<ObjectTag>
{
    public ObjectTag(string type, string? name)
    {
        this.Type = type;
        this.Name = name;
    }

    public readonly string Type;
    public readonly string? Name;

    public bool Equals(ObjectTag other)
    {
        return Type == other.Type && Name == other.Name;
    }

    public override bool Equals(object? obj)
    {
        return obj is ObjectTag other && Equals(other);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(Type, Name);
    }
}