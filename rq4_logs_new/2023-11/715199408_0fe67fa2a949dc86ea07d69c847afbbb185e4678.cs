using NerdStore.Core.DomainObjects;

namespace NerdStore.Catalog.Domain.ValueObjects;

public class Dimension
{
    public decimal Height { get; private set; }
    public decimal Width { get; private set; }
    public decimal Depth { get; private set; }

    public Dimension(
        decimal height,
        decimal width,
        decimal depth)
    {
        AssertionConcern.ValidateIfLessThan(height, 1, "O campo Altura não pode ser menor ou igual a 0");
        AssertionConcern.ValidateIfLessThan(width, 1, "O campo Largura não pode ser menor ou igual a 0");
        AssertionConcern.ValidateIfLessThan(depth, 1, "O campo Profundidade não pode ser menor ou igual a 0");

        Height = height;
        Width = width;
        Depth = depth;
    }

    public string FormattedDescription() => $"LxAxP: {Width} x {Height} x {Depth}";

    public override string ToString() => FormattedDescription();
}