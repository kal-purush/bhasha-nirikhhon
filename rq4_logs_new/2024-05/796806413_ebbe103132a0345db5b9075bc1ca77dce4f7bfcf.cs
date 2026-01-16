namespace Atmoos.Sphere.Test;

public static class Convenience
{
    public static IEnumerable<T> Shuffle<T>(this IEnumerable<T> values, Int32 seed = 23) => Shuffle(values, new Random(seed));
    public static IEnumerable<T> Shuffle<T>(this IEnumerable<T> values, Random random) => values.OrderBy(_ => random.Next());
}