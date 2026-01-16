using System.Reflection;

namespace VRCXDiscordTracker.Core.VRChat;

/// <summary>
/// VRChatのインスタンスの地域を表すクラス
/// </summary>
/// <param name="id">管理用ID</param>
/// <param name="token">トークン。例: "us"</param>
/// <param name="name">表示名</param>
internal class InstanceRegion(int id, string token, string name) : IComparable<InstanceRegion>, IEquatable<InstanceRegion>, IFormattable
{
    /// <summary>
    /// US West
    /// </summary>
    public static readonly InstanceRegion USWest = new(1, "us", "US West");

    /// <summary>
    /// US East
    /// </summary>
    public static readonly InstanceRegion USEast = new(2, "use", "US East");

    /// <summary>
    /// Europe
    /// </summary>
    public static readonly InstanceRegion Europe = new(3, "eu", "Europe");

    /// <summary>
    /// Japan
    /// </summary>
    public static readonly InstanceRegion Japan = new(4, "jp", "Japan");

    /// <summary>
    /// 管理用ID
    /// </summary>
    public readonly int Id = id;

    /// <summary>
    /// トークン
    /// </summary>
    /// <example>us</example>
    public readonly string Token = token;

    /// <summary>
    /// 表示名
    /// </summary>
    /// <example>US West</example>
    public readonly string Name = name;

    public static IEnumerable<T> GetAll<T>() where T : class
    {
        return typeof(T)
            .GetFields(BindingFlags.Public | BindingFlags.Static | BindingFlags.FlattenHierarchy)
            .Where(field => field.FieldType == typeof(T))
            .Select(field => field.GetValue(null) as T)
            .Where(instance => instance != null)!;
    }

    public static InstanceRegion? GetByToken(string? token) =>
        GetAll<InstanceRegion>().FirstOrDefault(region => region.Token.Equals(token, StringComparison.OrdinalIgnoreCase));

    public int CompareTo(InstanceRegion? other)
    {
        if (other == null) return 1;
        if (Id == other.Id) return 0;
        return Id.CompareTo(other.Id);
    }

    public override bool Equals(object? obj)
    {
        if (obj is InstanceRegion other)
            return Equals(other);

        return false;
    }

    public string ToString(string? format, IFormatProvider? formatProvider)
    {
        if (string.IsNullOrEmpty(format))
            return $"{GetType().Name}({Token})";
        if (format.Equals("id", StringComparison.OrdinalIgnoreCase))
            return Id.ToString(formatProvider);
        if (format.Equals("token", StringComparison.OrdinalIgnoreCase))
            return Token;
        if (format.Equals("name", StringComparison.OrdinalIgnoreCase))
            return Name;

        throw new FormatException($"The format '{format}' is not supported.");
    }

    public override int GetHashCode() => Id.GetHashCode();

    public bool Equals(InstanceRegion? other)
    {
        if (other == null) return false;
        if (Id == other.Id) return true;
        return false;
    }
}