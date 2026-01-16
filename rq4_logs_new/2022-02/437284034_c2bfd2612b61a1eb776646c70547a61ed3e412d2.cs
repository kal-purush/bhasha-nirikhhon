using STAIExtensions.Abstractions.Common;
using STAIExtensions.Abstractions.Data;
using STAIExtensions.Abstractions.DataContracts.Models;
using STAIExtensions.Abstractions.Views;
using STAIExtensions.Core.Views;
using STAIExtensions.Default.DataSets;
using STAIExtensions.Default.Helpers;

namespace STAIExtensions.Default.Views;

public class TracesView: DataSetView
{
    
    #region Constants

    private const string PARAM_CLOUDROLENAME = "CloudRoleName";
    private const string PARAM_CLOUDROLEINSTANCE = "CloudRoleInstance";

    #endregion
    
    #region Members

    private List<string>? _filterCloudRoleName;
    private List<string>? _filterCloudInstanceName;

    private List<Trace> _tracesFiltered;
    #endregion

    #region Properties

    public Dictionary<string, List<string>> CloudNames { get; private set; } = new();

    public List<Trace> TraceItems { get; private set; } = new();

    #endregion
    
    #region Overrides

    public override IEnumerable<DataSetViewParameterDescriptor>? ViewParameterDescriptors =>
        new List<DataSetViewParameterDescriptor>()
        {
            new DataSetViewParameterDescriptor(PARAM_CLOUDROLEINSTANCE, "string[]", false,
                "Filter values for the cloud role instance"),
            new DataSetViewParameterDescriptor(PARAM_CLOUDROLENAME, "string[]", false,
                "Filter values for the cloud role name"),
        };

    protected override Task BuildViewData(IDataSet dataSet)
    {
        if (dataSet is DataContractDataSet dataContractDataSet)
        {
            this.SetupParameters();

            this.LoadDistinctCloudNames(dataContractDataSet);

            this.BuildLocalListItems(dataContractDataSet);

            this.BuildViewItems(dataContractDataSet);
        }

        return Task.CompletedTask;
    }
    #endregion
    
    #region Private Methods
    /// <summary>
    /// Sets up the parameters for the view
    /// </summary>
    private void SetupParameters()
    {
        try
        {
            this._filterCloudRoleName =
                Helpers.ViewParameterHelper.ExtractParameter<List<string>>(this.ViewParameters, PARAM_CLOUDROLENAME);
            this._filterCloudInstanceName =
                Helpers.ViewParameterHelper.ExtractParameter<List<string>>(this.ViewParameters,
                    PARAM_CLOUDROLEINSTANCE);
        }
        catch (Exception ex)
        {
            Abstractions.Common.ErrorLoggingFactory.LogError(base.TelemetryClient, base.Logger, ex,
                "An error occured deserializing the view parameters: {ErrorMessage}", ex.Message);
        }
    }

    /// <summary>
    /// Loads a list of distinct Cloud Role Instance and Cloud Role Name values
    /// </summary>
    /// <param name="dataContractDataSet">The data set containing the original items</param>
    private void LoadDistinctCloudNames(DataContractDataSet dataContractDataSet)
    {
        var result = new List<string?>();

        result.AddRange(dataContractDataSet.Traces.Select(r => $"{r.CloudRoleInstance}*{r.CloudRoleName}")
            .Distinct());

        var distinctNames = result.Select(x => x).Distinct().ToList();
        var distinctSplitItems = distinctNames.Select(n => n?.Split("*")).Where(x => x != null).Select(n => new
        {
            Instance = n[0],
            Role = n[1]
        }).ToList();

        this.CloudNames.Clear();
        distinctSplitItems.ForEach(item =>
        {
            if (!this.CloudNames.ContainsKey(item.Instance))
                this.CloudNames[item.Instance] = new List<string>();
            this.CloudNames[item.Instance].Add(item.Role);
        });
    }

    /// <summary>
    /// Filters the records by the view filters
    /// </summary>
    private void BuildLocalListItems(DataContractDataSet dataContractDataSet)
    {
        // Keep a copy of the lists before after it is filtered 
        this._tracesFiltered = dataContractDataSet.Traces
            .FilterCloudRoleInstance(_filterCloudInstanceName)
            .FilterCloudRoleName(_filterCloudRoleName)
            .ToList();
    }

    /// <summary>
    /// Builds the output of the view items
    /// </summary>
    private void BuildViewItems(DataContractDataSet dataContractDataSet)
    {
        this.TraceItems = this._tracesFiltered
            .Where(t => t.RecordState == RecordState.New)
            .OrderByDescending(x => x.TimeStamp)
            .Take(200)
            .OrderBy(x => x.TimeStamp)
            .ToList();
    }
    #endregion
    
   
}