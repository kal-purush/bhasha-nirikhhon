using Atmoos.Sphere.Sync;
using BenchmarkDotNet.Attributes;

namespace Atmoos.Sphere.Benchmark;

[MemoryDiagnoser]
[ShortRunJob, WarmupCount(5), IterationCount(9)]
public class SynchronousAwaitBenchmark
{
    const ConfigureAwaitOptions continueOptions = ConfigureAwaitOptions.None;
    private static readonly TimeSpan delay = TimeSpan.FromMilliseconds(63);

    [Benchmark(Baseline = true)]
    public async Task AsyncAwaitTaskDelay() => await Task.Delay(delay).ConfigureAwait(continueOptions);

#pragma warning disable CS0618 // Type or member is obsolete
    [Benchmark]
    public void SyncAwaitTaskDelay() => Task.Delay(delay).Await(continueOptions);
#pragma warning restore CS0618 // Type or member is obsolete
}

/* Summary

BenchmarkDotNet v0.13.12, Arch Linux
Intel Core i7-8565U CPU 1.80GHz (Whiskey Lake), 1 CPU, 8 logical and 4 physical cores
.NET SDK 8.0.104
  [Host]   : .NET 8.0.4 (8.0.424.16909), X64 RyuJIT AVX2
  ShortRun : .NET 8.0.4 (8.0.424.16909), X64 RyuJIT AVX2

Job=ShortRun  IterationCount=9  LaunchCount=1  
WarmupCount=5  

| Method              | Mean     | Error    | Ratio | Allocated | Alloc Ratio |
|-------------------- |---------:|---------:|------:|----------:|------------:|
| AsyncAwaitTaskDelay | 63.71 ms | 0.551 ms |  1.00 |     464 B |        1.00 |
| SyncAwaitTaskDelay  | 63.34 ms | 0.350 ms |  1.00 |     360 B |        0.78 |
Summary */