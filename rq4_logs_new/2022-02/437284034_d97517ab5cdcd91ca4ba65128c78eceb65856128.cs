using STAIExtensions.Abstractions.DataContracts.Models;

namespace STAIExtensions.Default.Views.AvailabilityModels;

public class ViewAggregate
{

    #region Properties
    public DateTime StartDate { get; private set; }

    public DateTime EndDate { get; private set; }
        
    public double MaxDuration { get; private set; }
        
    public double MinDuration { get; private set; }
        
    public double AverageDuration { get; private set; }
        
    public int SuccessfulCount { get; private set; } 

    public int FailureCount { get; private set; }
        
    public int TotalCount { get; private set; }
        
    public double SuccessPercentage { get; private set; }

    #endregion

    #region ctor

    internal ViewAggregate(DateTime startDate, DateTime endDate)
    {
        this.EndDate = endDate;
        this.StartDate = startDate;
    }

    #endregion

    #region Methods
    internal void CalculateGroupItems(IEnumerable<Availability>? source)
    {
        if (source == null || source?.Count() == 0) return;
            
        var availabilities = source as Availability[] ?? source.ToArray();
        
        this.MaxDuration =  availabilities.Select(s => s.Duration ?? 0).Max();
        this.MinDuration =  availabilities.Select(s => s.Duration ?? 0).Min();
        this.AverageDuration = availabilities.Select(s => s.Duration ?? 0).Average();
        this.SuccessfulCount = availabilities
            .Count(s => string.Equals(s.Success ?? "", "1", StringComparison.OrdinalIgnoreCase));
        this.FailureCount = availabilities
            .Count(s => !string.Equals(s.Success ?? "", "1", StringComparison.OrdinalIgnoreCase));
        this.TotalCount = availabilities.Length;
        this.SuccessPercentage = TotalCount == 0 ? 0 : ((double)SuccessfulCount * 100d) / (double)TotalCount;
    }
    #endregion

}