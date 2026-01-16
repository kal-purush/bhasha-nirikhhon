using System.Globalization;
using System.Text.RegularExpressions;

namespace VideoMerger;

public class FFmpegProgressParser
{
    public event Action<double> ProgressChanged;

    private double _totalDuration;

    public FFmpegProgressParser(double totalDuration)
    {
        _totalDuration = totalDuration;
    }

    public void Parse(string data)
    {
        if (string.IsNullOrWhiteSpace(data))
            return;

        // Sample progress output: frame=  220 fps=0.0 q=-1.0 Lsize=   13763kB time=00:00:09.00 bitrate=12522.5kbits/s speed=15.9x
        var timeMatch = Regex.Match(data, @"time=(\d{2}:\d{2}:\d{2}.\d{2})");
        if (timeMatch.Success)
        {
            var currentTime = TimeSpan.ParseExact(timeMatch.Groups[1].Value, @"hh\:mm\:ss\.ff",
                CultureInfo.InvariantCulture);

            // Calculate the progress percentage
            double progress = (currentTime.TotalSeconds / _totalDuration) * 100;
            progress = Math.Min(progress, 100); // Cap the progress at 100%

            // Trigger the event
            ProgressChanged?.Invoke(progress);
        }
    }
}