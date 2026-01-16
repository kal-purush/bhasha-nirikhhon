using Atmoos.Sphere.Time;

using System.Diagnostics;

namespace Atmoos.Sphere.Test.Time.Extensions;

public class TimeSpanAsStreamTest
{
    [Fact]
    public void PassingANegativeInterval_ToTheConstructor_ThrowsAnArgumentOutOfRangeException()
    {
        TimeSpan interval = TimeSpan.FromMilliseconds(-5);
        Assert.Throws<ArgumentOutOfRangeException>(() => interval.AsStream());
    }

    [Fact]
    public void PassingAnIntervalOfZero_ToTheConstructor_ThrowsAnArgumentOutOfRangeException()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => TimeSpan.Zero.AsStream());
    }

    [Fact]
    public async Task Cancellation_ThrowsTaskCanceledException()
    {
        var count = 0;
        using var source = new CancellationTokenSource(200);
        await Assert.ThrowsAsync<TaskCanceledException>(async () =>
        {
            await foreach (TimeSpan timeStamp in TimeSpan.FromMilliseconds(10).AsStream(source.Token)) {
                if (count++ > 3) {
                    source.Cancel();
                }
            }
        });
    }

    [Fact]
    public async Task GeneratedTimers_CompleteAtRegularIntervals_GivenNoConcurrentJob()
    {
        const Int32 steps = 5;
        var interval = TimeSpan.FromMilliseconds(40);
        var track = new TimerTrack();
        await RunTrack(track, steps, interval);
        var expectedTimeStamps = Enumerable.Range(0, steps).Select(i => i * interval).ToList();
        Assert.Equal(expectedTimeStamps, track.JitterFreeTimeStamps);
    }

    [Fact]
    public async Task GeneratedTimers_CompleteExpectedWallClockTime_GivenNoConcurrentJob()
    {
        var tol = TimeSpan.FromMilliseconds(5);
        const Int32 steps = 13;
        var interval = TimeSpan.FromMilliseconds(30);
        var track = new TimerTrack();
        await RunTrack(track, steps, interval);
        var expectedTimeStamps = Enumerable.Range(0, steps).Select(i => i * interval).ToList();
        AreApproximatelyEqual(expectedTimeStamps, track.TimeKeeperTimeStamps, tol);
    }

    [Fact]
    public async Task GeneratedTimers_CompleteAtRegularIntervals_GivenShortConcurrentJob()
    {
        const Int32 steps = 5;
        const Int32 jobDurationMs = 30;
        var interval = TimeSpan.FromMilliseconds(2 * jobDurationMs);
        var track = new TimerTrack(async (_, ct) => await Task.Delay(jobDurationMs, ct).ConfigureAwait(false));
        await RunTrack(track, steps, interval);
        var expectedTimeStamps = Enumerable.Range(0, steps).Select(i => i * interval).ToList();
        Assert.Equal(expectedTimeStamps, track.JitterFreeTimeStamps);
    }

    [Fact]
    public async Task GeneratedTimers_CompleteAtRegularIntervals_GivenLongRunningConcurrentJob()
    {
        const Int32 steps = 5;
        const Int32 jobDurationMs = 45;
        var interval = TimeSpan.FromMilliseconds(50);
        var track = new TimerTrack(async (_, ct) => await Task.Delay(jobDurationMs, ct).ConfigureAwait(false));
        await RunTrack(track, steps, interval);
        var expectedTimeStamps = Enumerable.Range(0, steps).Select(i => i * interval).ToList();
        Assert.Equal(expectedTimeStamps, track.JitterFreeTimeStamps);
    }

    [Fact]
    public async Task GeneratedTimers_CompleteAtIntegerMultiplesOfInterval_GivenLongRunningConcurrentJob()
    {
        const Int32 steps = 9;
        const Int32 jobDurationMs = 25;
        const Int32 jobLongDurationMs = 30;
        var interval = TimeSpan.FromMilliseconds(2 * jobDurationMs);
        var track = new TimerTrack(job);
        await RunTrack(track, steps, interval);
        Int32[] expectedTimeStamps = [0, 50, 100, 200, 250, 300, 400, 450, 500];
        Assert.Equal(expectedTimeStamps.Select(t => TimeSpan.FromMilliseconds(t)), track.JitterFreeTimeStamps);

        static async Task job(Int32 index, CancellationToken token)
        {
            if (index == 2 || index == 5) {
                await Task.Delay(jobLongDurationMs, token).ConfigureAwait(false);
            }
            await Task.Delay(jobDurationMs, token).ConfigureAwait(false);
        }
    }

    [Fact]
    public async Task GeneratedTimers_CompleteAtIntegerMultiplesOfInterval_GivenExcessivelyLongRunningConcurrentJob()
    {
        const Int32 steps = 6;
        const Int32 jobDurationMs = 30;
        var interval = TimeSpan.FromMilliseconds(2 * jobDurationMs);
        var jobLongDurationMs = 3 * interval + TimeSpan.FromMilliseconds(10);
        Func<Int32, CancellationToken, Task> job = async (index, token) =>
        {
            if (index == 2) {
                await Task.Delay(jobLongDurationMs, token).ConfigureAwait(false);
            }
            await Task.Delay(jobDurationMs, token).ConfigureAwait(false);
        };
        TimerTrack track = new TimerTrack(job);
        await RunTrack(track, steps, interval);
        var expectedTimeStamps = (new Int32[] { 0, 1, 2, 6, 7, 8 }).Select(i => i * interval);
        Assert.Equal(expectedTimeStamps, track.JitterFreeTimeStamps);
    }


    [Fact]
    public async Task GeneratedTimers_CompleteAtIntegerMultiplesOfInterval_ComparedToWallClock_GivenExcessivelyLongRunningConcurrentJob()
    {
        const Int32 steps = 9;
        const Int32 jobDurationMs = 25;
        const Int32 jobLongDurationMs = 30;
        var tol = TimeSpan.FromMilliseconds(10);
        var interval = TimeSpan.FromMilliseconds(2 * jobDurationMs);
        Func<Int32, CancellationToken, Task> job = async (index, token) =>
        {
            if (index == 2 || index == 5) {
                await Task.Delay(jobLongDurationMs, token).ConfigureAwait(false);
            }
            await Task.Delay(jobDurationMs, token).ConfigureAwait(false);
        };
        TimerTrack track = new TimerTrack(job);
        await RunTrack(track, steps, interval);
        Int32[] expectedTimeStamps = [0, 50, 100, 200, 250, 300, 400, 450, 500];
        AreApproximatelyEqual(expectedTimeStamps, track.TimeKeeperTimeStamps, tol);
    }

    private static async Task RunTrack(TimerTrack track, Int32 steps, TimeSpan interval)
    {
        try {
            await RunRawTrack(track, steps, interval).ConfigureAwait(false);
        }
        catch (TaskCanceledException) {
            // Totally Expected :-)
        }
    }

    private static Task RunRawTrack(TimerTrack track, Int32 steps, TimeSpan interval, CancellationToken token = default)
    {
        return track.Run(interval.AsStream(token), steps, token);
    }

    private static void AreApproximatelyEqual(IEnumerable<Int32> expectedMs, IEnumerable<TimeSpan> actual, TimeSpan tol)
        => AreApproximatelyEqual(expectedMs.Select(ms => TimeSpan.FromMilliseconds(ms)), actual, tol);
    private static void AreApproximatelyEqual(IEnumerable<TimeSpan> expected, IEnumerable<TimeSpan> actual, TimeSpan tol)
    {
        List<TimeSpan> actualList = actual.ToList();
        List<TimeSpan> expectedList = expected.ToList();
        IEnumerable<TimeSpan> deltas = expectedList.Zip(actualList, (e, a) => e >= a ? (e - a) : (a - e));
        TimeSpan maxDelta = deltas.Max();
        if (maxDelta <= tol) {
            Assert.True(true);
            return;
        }
        Assert.Equal(expectedList, actualList);
    }

    private class TimerTrack
    {
        private readonly Func<Int32, CancellationToken, Task> workloadSimulator;

        public List<TimeSpan> ElapsedIntervals { get; } = [];

        public List<TimeSpan> JitterFreeTimeStamps { get; } = [];

        public List<TimeSpan> TimeKeeperTimeStamps { get; } = [];

        public TimerTrack() : this((_, __) => Task.CompletedTask) { }
        public TimerTrack(Func<Int32, CancellationToken, Task> workloadSimulator)
        {
            this.workloadSimulator = workloadSimulator;
        }

        public async Task Run(IAsyncEnumerable<TimeSpan> timers, Int32 steps, CancellationToken token)
        {
            try {
                Int32 count = 0;
                TimeSpan prevTimeStamp = TimeSpan.Zero;
                Stopwatch timeKeeper = Stopwatch.StartNew();
                await foreach (var timeStamp in timers.WithCancellation(token)) {
                    TimeSpan elapsedTime = timeKeeper.Elapsed;
                    ElapsedIntervals.Add(timeStamp - prevTimeStamp);
                    JitterFreeTimeStamps.Add(timeStamp);
                    TimeKeeperTimeStamps.Add(elapsedTime);
                    await this.workloadSimulator(count++, token).ConfigureAwait(false);
                    if (count >= steps) {
                        return;
                    }
                    prevTimeStamp = timeStamp;
                }
            }
            finally {
                this.ElapsedIntervals.RemoveAt(0);
            }
        }
    }
}
