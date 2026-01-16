namespace MDLibrary.Models;

public sealed class MethodMetadata : BaseMetadata
{
    public string ClassName { get; set; }
    public string Remarks { get; set; }
    public string Returns { get; set; }
    public string Example { get; set; }
    public List<ExceptionMetadata> Exceptions { get; set; } = new List<ExceptionMetadata>();
    public List<ParameterMetadata> Parameters { get; set; } = new List<ParameterMetadata>();
    public List<ResponseMetadata> Responses { get; set; } = new List<ResponseMetadata>();
}