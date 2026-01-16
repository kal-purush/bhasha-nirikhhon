namespace Atmoos.Sphere.Sync;

public static class Extensions
{
    [Obsolete("Please use async await instead")]
    public static void Await(this Task task, ConfigureAwaitOptions options = ConfigureAwaitOptions.None)
        => task.ConfigureAwait(options).GetAwaiter().GetResult();

    [Obsolete("Please use async await instead")]
    public static void Await<T>(this Task<T> task, ConfigureAwaitOptions options = ConfigureAwaitOptions.None)
        => task.ConfigureAwait(options).GetAwaiter().GetResult();
}