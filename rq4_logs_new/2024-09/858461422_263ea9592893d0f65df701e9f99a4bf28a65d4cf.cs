using Aptem.Warehouse.Inventory.Counter.InventoryCounter;

namespace Aptem.Warehouse.Inventory.Counter.Tests;

public class InventoryCounterV1Tests
{
    private readonly InventoryCounterV1 parser;

    public InventoryCounterV1Tests()
    {
        parser = new InventoryCounterV1();
    }

    [Fact]
    public void CanParse_ValidPart1Input_ReturnsTrue()
    {
        const string input = "aaaa bb c";
        Assert.True(parser.CanParse(input));
    }

    [Fact]
    public void CanParse_Part2Input_ReturnsFalse()
    {
        const string input = "#p2# 4a 2b 1c";
        Assert.False(parser.CanParse(input));
    }

    [Fact]
    public void Parse_ValidInput_ReturnsCorrectCounts()
    {
        const string input = "aaaa bb c";
        Dictionary<char, int> expected = new()
        {
            { 'a', 4 },
            { 'b', 2 },
            { 'c', 1 }
        };
        Dictionary<char, int> result = parser.Parse(input);
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Parse_InputWithNonLetters_IgnoresNonLetters()
    {
        const string input = "a1a2a3 bb$% c!";
        Dictionary<char, int> expected = new()
        {
            { 'a', 3 },
            { 'b', 2 },
            { 'c', 1 }
        };
        Dictionary<char, int> result = parser.Parse(input);
        Assert.Equal(expected, result);
    }
}