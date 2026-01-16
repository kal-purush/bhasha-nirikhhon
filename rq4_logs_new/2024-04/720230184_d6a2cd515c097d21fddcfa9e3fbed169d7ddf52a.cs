using ProductManagementSystem.StevieTV.Data;
using ProductManagementSystem.StevieTV.Models;

namespace ProductManagementSystem.StevieTV.Helpers;

public class CustomLogger : ILogger
{
    private readonly VideoGameContext _context;

    public CustomLogger(VideoGameContext context)
    {
        _context = context;
    }

    public IDisposable BeginScope<TState>(TState state) => null;

    public bool IsEnabled(LogLevel logLevel) => true;
    
    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        if (logLevel == LogLevel.None)
            return;

        var errorLog = new ErrorLog()
        {
            Error = $"{logLevel.ToString()}: {formatter(state, exception)}",
            Time = DateTime.Now
        };

        _context.ErrorLog.Add(errorLog);
        _context.SaveChanges();
        
    }
}