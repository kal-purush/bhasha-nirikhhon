namespace NLogFlake;

public interface ILogFlake
{
    void SendLog(string content, Dictionary<string, object>? parameters = null);

    void SendLog(LogLevels level, string content, Dictionary<string, object>? parameters = null);

    void SendLog(LogLevels level, string? correlation, string? content, Dictionary<string, object>? parameters = null);

    void SendException(Exception e);

    void SendException(Exception e, string? correlation);

    void SendPerformance(string label, long duration);

    IPerformanceCounter MeasurePerformance(string label);
}