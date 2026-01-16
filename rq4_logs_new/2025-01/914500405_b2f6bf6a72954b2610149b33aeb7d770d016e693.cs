namespace Prototype.Motorbikes;

/// <summary>
/// Represents the classic bike subclass.
/// </summary>
/// <param name="classicBike">The <see cref="ClassicBike"/> object.</param>
internal sealed class ClassicBike(ClassicBike classicBike) : Motorbike(classicBike)
{
    /// <summary>
    /// The headlight type.
    /// </summary>
    public string? HeadlightType { get; set; } = classicBike.HeadlightType;

    /// <inheritdoc />
    internal override Motorbike Clone()
        => new ClassicBike(this);
}