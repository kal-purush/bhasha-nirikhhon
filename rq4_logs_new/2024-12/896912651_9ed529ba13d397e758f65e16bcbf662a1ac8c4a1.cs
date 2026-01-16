using aoc.day3;

namespace aoc.test;

public class Day3Test
{
    [Test]
    public void Day3Part1Easy()
    {
        const string input = "mul(3,9)";
        Assert.That(Day3.Part1Calculation(input), Is.EqualTo(27));
    }   
    [Test]
    public void Day3Part1Hard()
    {
        const string input = "xmul(2,4)%&mul[3,7]!@^do_not_mul(5,5)+mul(32,64]then(mul(11,8)mul(8,5))";
        Assert.That(Day3.Part1Calculation(input), Is.EqualTo(161));
    }
    
    
    [Test]
    public void Day3Part2Hard()
    {
        const string input = "xmul(2,4)&mul[3,7]!^don't()_mul(5,5)+mul(32,64](mul(11,8)undo()?mul(8,5))";
        Assert.That(Day3.Part2Calculation(input), Is.EqualTo(48));
    }
    
}