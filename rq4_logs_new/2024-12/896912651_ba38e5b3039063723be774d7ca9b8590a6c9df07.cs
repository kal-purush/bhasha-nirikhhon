using aoc.day4;

namespace aoc.test;


[TestFixture]
public class Day4Tests
{
    private readonly List<string> _grid =
    [
        "MMMSXXMASM",
        "MSAMXMSMSA",
        "AMXSXMAAMM",
        "MSAMASMSMX",
        "XMASAMXAMM",
        "XXAMMXXAMA",
        "SMSMSASXSS",
        "SAXAMASAAA",
        "MAMMMXMMMM",
        "MXMXAXMASX"
    ];
    
    
    [Test]
    public void FindXmasCount()
    {
        var day4 = new Day4(_grid);
        Assert.That(day4.Part1Count, Is.EqualTo(18));
    }


    [Test]
    public void FindXmasCountSimple()
    {
        List<string> simpList = new List<string>
        {
            "XMAS",
            "MXMA",
            "AMXM",
            "SAMX"
        };
        var day4 = new Day4(simpList);
        Assert.That(day4.Part1Count, Is.EqualTo(4));
    }
    
    [Test]
    public void FindXmasCountDiagonal()
    {
        List<string> simpList = new List<string>
        {
            "X..S",
            ".MA.",
            ".MA.",
            "X..S"
        };
        var day4 = new Day4(simpList);
        Assert.That(day4.Part1Count, Is.EqualTo(2));
    }
    
    [Test]
    public void FindXmasCountDiagonal2()
    {
        List<string> simpList = new List<string>
        {
            "X..X",
            ".MM.",
            ".AA.",
            "S..S"
        };
        var day4 = new Day4(simpList);
        Assert.That(day4.Part1Count, Is.EqualTo(2));
    }
    [Test]
    public void FindXmasCountDiagonalInverse()
    {
        List<string> simpList = new List<string>
        {
            "S..S",
            ".AA.",
            ".MM.",
            "X..X"
        };
        var day4 = new Day4(simpList);
        Assert.That(day4.Part1Count, Is.EqualTo(2));
    }

    [Test]
    public void FindProblem()
    {
        List<string> simpList = new List<string>
        {
            "X...",
            ".M..",
            "..A.",
            "...S"
        };
    }
    
    
    [Test]
    public void FindPart2Count()
    {
        var day4 = new Day4(_grid);
        Assert.That(day4.Part2Count, Is.EqualTo(9));
    }
}