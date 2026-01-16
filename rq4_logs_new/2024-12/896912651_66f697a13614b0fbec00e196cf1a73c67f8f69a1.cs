using aoc.day7;

namespace aoc.test;

public class Day7Tests
{
    // 190: 10 19
    // 3267: 81 40 27
    // 83: 17 5
    // 156: 15 6
    // 7290: 6 8 6 15
    // 161011: 16 10 13
    // 192: 17 8 14
    // 21037: 9 7 18 13
    // 292: 11 6 16 20
    private readonly string[] _input =
    [
        "190: 10 19",
        "3267: 81 40 27",
        "83: 17 5",
        "156: 15 6",
        "7290: 6 8 6 15",
        "161011: 16 10 13",
        "192: 17 8 14",
        "21037: 9 7 18 13",
        "292: 11 6 16 20"
    ];
    
    [Test]
    public void Day1()
    {
        Day7 day7 = new(_input);
        long result = day7.Part1();
        Assert.That(result, Is.EqualTo(3749));
    }
    
    private readonly string[] _input2 =
    {
        "2382106471: 2 8 175 1 17 3 5 9 4 51 5",
        "864708004: 278 22 259 2 12 3",
        "1659517050: 33 8 9 673 51 967",
        "5331437: 5 3 305 89 849",
        "97828: 8 6 6 3 5 70 4 696 5 91 3",
        "793977: 7 93 977",
        "21316689: 8 8 3 833 67 22 19 19",
        "4685569: 3 5 3 9 9 3 4 1 746 951 7",
        "4635637004: 7 1 9 9 87 8 14 7 8 1 78 9",
        "367804926120: 9 900 129 43 9 8 57 40",
        "3977374: 397 7 37 4",
        "28883667: 640 6 1 9 5 2 7 59 1 6 93",
        "1219750: 7 614 28 37 5 7 2"
    };
    
    
    
    [Test]
    public void Day1_fuckyou()
    {
        Day7 day7 = new(_input2);
        long result = day7.Part1();
        Assert.That(result, Is.EqualTo(1715720553));
    }

    [Test]
    public void Day1Line7()
    {
        var input = new string[]
        {
            "21316689: 8 8 3 833 67 22 19 19"
        };
        Day7 day7 = new(input);
        long result = day7.Part1();
        Assert.That(result, Is.EqualTo(21316689));
    }  
    [Test]
    public void Day1Line2()
    {
        var input = new string[]
        {
            "1659517050: 33 8 9 673 51 967",
            "97828: 8 6 6 3 5 70 4 696 5 91 3",
        };
        Day7 day7 = new(input);
        long result = day7.Part1();
        Assert.That(result, Is.EqualTo(1659614878));
    }

    [Test]
    public void Day1First100()
    {
        var file = "./inputs/day7_100.txt";
        Day7 day7 = new(file);
        long result = day7.Part1();
        Assert.That(result, Is.EqualTo(27658973736));
    }
}