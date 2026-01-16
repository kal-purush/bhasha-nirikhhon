namespace CloudFabric.Projections.Queries;

public class SortInfo
{
    /// <summary>
    /// Path to the sorted key, e.g. User.Profile.FirstName
    /// </summary>
    public string KeyPath { get; set; }
    
    /// <summary>
    /// asc or desc
    /// </summary>
    public string Order { get; set; }

    // NOTE: compares only using EQUAL operation
    
    /// <summary>
    /// A list of values to filter objects used in sorting.
    /// E.g. you have an array and you need to use a specific element of the array in sorting.
    /// </summary>
    public List<SortingFilter> Filters { get; set; } = new();
}

public class SortingFilter
{
    /// <summary>
    /// Path to the filtered property, e.g. User.Products.Name
    /// </summary>
    public string FilterKeyPath { get; set; }
    
    /// <summary>
    /// Value of the filtered property, e.g. "product_name"
    /// </summary>
    public object FilterValue { get; set; }
}