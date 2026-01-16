using System.Diagnostics;
using Atmoos.Sphere.Sync;

namespace Atmoos.Sphere.Test.Sync;

public class ExtensionsTest
{
    private static readonly TimeSpan delay = TimeSpan.FromMilliseconds(12);

    [Fact]
    public void AwaitVoidCompletes()
    {
        Task task = SomeTask(delay);

#pragma warning disable CS0618 // Type or member is obsolete
        task.Await();
#pragma warning restore CS0618 // Type or member is obsolete

        Assert.True(task.IsCompleted);
    }

    [Fact]
    public void AwaitResultCompletes()
    {
        Task<TimeSpan> task = SomeTask(delay);

#pragma warning disable CS0618 // Type or member is obsolete
        var actual = task.Await();
#pragma warning restore CS0618 // Type or member is obsolete

        Assert.True(task.IsCompleted);
        Assert.Equal(delay, actual);
    }

    [Fact]
    public void AwaitVoidFailsWithSingularException()
    {
        var expectedMessage = "Some message";
        Task task = SomeFailingTask(expectedMessage);

#pragma warning disable CS0618 // Type or member is obsolete
        var error = Assert.Throws<InvalidOperationException>(() => task.Await());
#pragma warning restore CS0618 // Type or member is obsolete

        Assert.Equal(expectedMessage, error.Message);
    }

    [Fact]
    public void AwaitResultFailsWithSingularException()
    {
        var expectedMessage = "Some message";
        Task<TimeSpan> task = SomeFailingTask(expectedMessage);

#pragma warning disable CS0618 // Type or member is obsolete
        var error = Assert.Throws<InvalidOperationException>(() => task.Await());
#pragma warning restore CS0618 // Type or member is obsolete

        Assert.Equal(expectedMessage, error.Message);
    }

    [Fact]
    public void AwaitVoidPropagatesCancellation()
    {
        var longDelay = 4 * delay;
        var cancelAfter = 1.2 * delay;
        using var cts = new CancellationTokenSource(cancelAfter);
        Task task = SomeTask(longDelay, cts.Token);
        var timer = Stopwatch.StartNew();

#pragma warning disable CS0618 // Type or member is obsolete
        Assert.Throws<TaskCanceledException>(() => task.Await());
#pragma warning restore CS0618 // Type or member is obsolete

        Assert.InRange(timer.Elapsed, delay, longDelay);
    }

    [Fact]
    public void AwaitResultPropagatesCancellation()
    {
        var longDelay = 4 * delay;
        var cancelAfter = 1.2 * delay;
        using var cts = new CancellationTokenSource(cancelAfter);
        Task<TimeSpan> task = SomeTask(longDelay, cts.Token);
        var timer = Stopwatch.StartNew();

#pragma warning disable CS0618 // Type or member is obsolete
        Assert.Throws<TaskCanceledException>(() => task.Await());
#pragma warning restore CS0618 // Type or member is obsolete

        Assert.InRange(timer.Elapsed, delay, longDelay);
    }

    private static async Task<TimeSpan> SomeTask(TimeSpan delay, CancellationToken token = default)
    {
        await Task.Delay(delay, token).ConfigureAwait(false);
        return delay;
    }

    private static async Task<TimeSpan> SomeFailingTask(String message)
    {
        await Task.Yield();
        throw new InvalidOperationException(message);
    }
}
