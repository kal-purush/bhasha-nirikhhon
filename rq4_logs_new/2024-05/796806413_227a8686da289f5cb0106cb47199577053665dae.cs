using Atmoos.Sphere.Collections;

namespace Atmoos.Sphere.Test.Collections;

public class ExtensionsTest
{
    [Fact]
    public void TheDifferencesOnLinearFunctionAreConstant()
    {
        Int32 constant = 5;
        var linearFunction = Enumerable.Range(0, 10).Select(i => i * constant).ToArray();

        var actual = linearFunction.Differences().ToArray();

        Assert.All(actual, d => Assert.Equal(constant, d));
        Assert.Equal(linearFunction.Length - 1, actual.Length);
    }

    [Fact]
    public void TheDifferencesOnQuadraticFunctionAreLinearFunction()
    {
        const Int32 count = 20;
        Int32 constant = 2;
        var linearFunction = Enumerable.Range(0, count - 1).Select(i => 2 * i * constant + constant).ToArray();
        var quadraticFunction = Enumerable.Range(0, count).Select(i => i * i * constant).ToArray();

        var actual = quadraticFunction.Differences().ToArray();

        Assert.Equal(linearFunction, actual);
    }

    [Fact]
    public void TheSlidingWindowFunction()
    {
        const Int32 count = 20;
        var data = Enumerable.Range(0, count).ToArray();
        var expected = Enumerable.Range(0, count - 1).Select(i => $"{i}{i + 1}").ToArray();

        var actual = data.Window((a, b) => $"{a}{b}").ToArray();

        Assert.Equal(expected, actual);
    }
}