using aoc.day2;
using NUnit.Framework;
using System.Threading.Tasks;

namespace aoc.test;

[TestFixture]
public class Day2Tests
{
    private readonly string[] _input =
    {
        "7 6 4 2 1",
        "1 2 7 8 9",
        "9 7 6 2 1",
        "1 3 2 4 5",
        "8 6 4 4 1",
        "1 3 6 7 9"
    };

    [Test]
    public async Task Part1Shouldpass()
    {
        Assert.That(await Day2.Part1Calculation(_input), Is.EqualTo(2));
    }

    [Test]
    public async Task IncreaseToHight_1()
    {
        Assert.That(await Day2.Part1Calculation(new[] { "1 2 7 8 9" }), Is.EqualTo(0));
    }

    [Test]
    public async Task IncreaseToHight_2()
    {
        Assert.That(await Day2.Part1Calculation(new[] { "7 6 4 2 1" }), Is.EqualTo(1));
    }

    [Test]
    public async Task IncreaseToHight_3()
    {
        Assert.That(await Day2.Part1Calculation(new[] { "9 7 6 2 1" }), Is.EqualTo(0));
    }

    [Test]
    public async Task IncreaseToHight_4()
    {
        Assert.That(await Day2.Part1Calculation(new[] { "1 3 2 4 5" }), Is.EqualTo(0));
    }

    [Test]
    public async Task IncreaseToHight_5()
    {
        Assert.That(await Day2.Part1Calculation(new[] { "8 6 4 4 1" }), Is.EqualTo(0));
    }

    [Test]
    public async Task IncreaseToHight_6()
    {
        Assert.That(await Day2.Part1Calculation(new[] { "1 3 6 7 9" }), Is.EqualTo(1));
    }
    
    [Test]
    public async Task Part2ShouldPass()
    {
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await Day2.Part2Calculation(new[] { "7 6 4 2 1" }), Is.EqualTo(1));
            Assert.That(await Day2.Part2Calculation(new[] { "1 2 7 8 9" }), Is.EqualTo(0));
            Assert.That(await Day2.Part2Calculation(new[] { "9 7 6 2 1" }), Is.EqualTo(0));
            Assert.That(await Day2.Part2Calculation(new[] { "1 3 2 4 5" }), Is.EqualTo(1));
            Assert.That(await Day2.Part2Calculation(new[] { "8 6 4 4 1" }), Is.EqualTo(1));
            Assert.That(await Day2.Part2Calculation(new[] { "1 3 6 7 9" }), Is.EqualTo(1));
        });
    }
}