using System.Text.Json.Serialization;

namespace ReportPortal_APIClient.Abstractions.Models
{
    public class WidgetModel
    {
        [JsonPropertyName("share")]
        public bool Share { get; set; }

        [JsonPropertyName("widgetId")]
        public int WidgetId { get; set; }

        [JsonPropertyName("widgetName")]
        public string WidgetName { get; set; }

        [JsonPropertyName("widgetOptions")]
        public object WidgetOptions { get; set; }

        [JsonPropertyName("widgetType")]
        public string WidgetType { get; set; }
    }
}