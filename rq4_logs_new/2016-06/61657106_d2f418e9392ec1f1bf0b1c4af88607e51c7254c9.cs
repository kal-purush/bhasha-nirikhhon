using System;

namespace Bands.Time
{
    /// <summary>
    /// A serializable function call containing a collection which will be timed. In addition to the total time, the
    /// average time of completion per list item will be calculated. An errand contains the appropriate information 
    /// for executing some functionality.
    /// </summary>
    public interface ITimedCollectionErrand : IErrand
    {
        int CollectionCount { get; }
        TimeSpan TimeToCompleteEntireCollectionErrand { get; set; }
        TimeSpan AverageTimeToCompleteEachCollectionItem { get; set; }
    }
}