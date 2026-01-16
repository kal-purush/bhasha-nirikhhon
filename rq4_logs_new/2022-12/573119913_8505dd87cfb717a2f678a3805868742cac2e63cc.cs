using System.Timers;
using Timer = System.Timers.Timer;

namespace VanKassa.Presentation.Admin.Data;

public class SearchValueDelayer : IDisposable
{
    private Timer _timer;

    private string _searchValue = string.Empty;

    public EventHandler<string>? Delayed;

    public SearchValueDelayer(int delayInterval)
    {
        _timer = new Timer(delayInterval);
        _timer.Elapsed += OnElapsed;
        _timer.AutoReset = false;
    }

    private void OnElapsed(object? source, ElapsedEventArgs args)
        => Delayed?.Invoke(this, _searchValue);

    public void Update(string value)
    {
        _timer.Stop();
        _searchValue = value;
        _timer.Start();
    }
    
    public void Dispose()
    {
        _timer.Stop();
        _timer.Elapsed -= OnElapsed;
        _timer.Dispose();
    }
}