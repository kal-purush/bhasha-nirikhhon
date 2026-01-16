using Atmoos.Sphere.Async;

namespace Atmoos.Sphere.Test.Async;

public class ContextFlowTest
{
    [Fact]
    public async Task BasicContextSwitchingOnExplicitlySetContexts()
    {
        var expected = SetContext(new SomeContext());
        var flow = ContextFlow.Current();
        Assert.NotNull(expected);
        await Task.Yield();
        SetContext(new SomeOtherContext());
        await AsynchronousStuff().ConfigureAwait(false);
        Assert.NotEqual(expected, SynchronizationContext.Current);
        await flow;
        Assert.Same(expected, SynchronizationContext.Current);
    }

    [Fact]
    public async Task BasicContextSwitchingOnNoContext_PreservesNonExistantContext()
    {
        const Int32 expectedNumber = 23;
        var number = expectedNumber - 12; // i.e. something else than expected...
        var expected = SynchronizationContext.Current;
        var flow = ContextFlow.Current();
        var task = Task.Run(() => number = expectedNumber); // simulate stuff...
        await Task.Yield(); // and more stuff
        await task.ConfigureAwait(false);
        await flow;
        Assert.Same(expected, SynchronizationContext.Current);
        Assert.Equal(expectedNumber, number); // somewhat silly sanity check
    }

    [Fact]
    public async Task ContextCanBeUsedMultipleTimesOnMultipleContexts()
    {
        const Int32 iterations = 12;
        var expected = SetContext(new SomeContext());
        var flow = ContextFlow.Current();
        for (Int32 iteration = 0; iteration < iterations; ++iteration) {
            SetContext(iteration % 2 == 0 ? new SomeOtherContext() : new AnotherContext());
            await AsynchronousStuff().ConfigureAwait(false);
            Assert.NotEqual(expected, SynchronizationContext.Current);
            await flow; // we're awaiting the same context over and over!
            Assert.Same(expected, SynchronizationContext.Current);
        }
    }

    private static async Task AsynchronousStuff()
    {
        const Int32 initial = 3;
        var number = initial;
        await Task.Yield();
        var parallelStuff = Task.Run(() => number += 3);
        var moreParallelStuff = Task.Delay(12);
        await Task.WhenAll(parallelStuff, moreParallelStuff).ConfigureAwait(false);
        await parallelStuff.ConfigureAwait(false);
        await moreParallelStuff.ConfigureAwait(false);
        await Task.Yield();
        Assert.NotEqual(initial, number);
    }

    private static SynchronizationContext SetContext(SynchronizationContext context)
    {
        SynchronizationContext.SetSynchronizationContext(context);
        return SynchronizationContext.Current ?? context;
    }

    private sealed class SomeContext : SynchronizationContext
    {
        public override Boolean Equals(Object? obj) => obj is SomeContext c && ReferenceEquals(this, c);
        public override Int32 GetHashCode() => typeof(SomeContext).GetHashCode();
    }
    private sealed class SomeOtherContext : SynchronizationContext { }
    private sealed class AnotherContext : SynchronizationContext { }
}