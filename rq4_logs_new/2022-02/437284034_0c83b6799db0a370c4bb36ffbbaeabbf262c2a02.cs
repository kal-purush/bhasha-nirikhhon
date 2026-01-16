namespace STAIExtensions.Default.Views.CustomMetricModels;


public class ViewAggregate
{

    #region Properties
    public int? NumberOfCalls { get; private set; }

    public DateTime? StartDate { get; private set; }
        
    public DateTime? EndDate { get; private set; }
    #endregion

    #region ctor

    public ViewAggregate(int? numberOfCalls, DateTime? startDate, DateTime? endDate)
    {
        this.NumberOfCalls = numberOfCalls;
        this.StartDate = startDate;
        this.EndDate = endDate;
    }
    

    #endregion
    
}