using aoc;

namespace day1.test;

public class Tests
{
    private readonly List<int> _leftList = [3,4,2,1,3,3];
    private readonly List<int> _rightList = [4,3,5,3,9,3];
    

    [Test]
    public void Day1_Success()
    {
        const int expected = 11;
        var result = Day1.AddDistances(_leftList, _rightList);
        Assert.That(result, Is.EqualTo(expected));
    }

    [Test]
    public void Day1_similarity_success()
    {
        const int exptected = 31;
        Assert.That(Day1.CalculateSimilarityScore(_leftList, _rightList), Is.EqualTo(exptected));
    }  
    
    [Test]
    public void Day1_similarity_success_oneliner()
    {
        const int exptected = 31;
        Assert.That(Day1.CalulateSimilarityScoreOneLiner(_leftList, _rightList), Is.EqualTo(exptected));
    }
    
    
    
}