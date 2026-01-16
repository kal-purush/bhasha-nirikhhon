using Newtonsoft.Json;
using System.ComponentModel.DataAnnotations.Schema;

namespace ProvidersDomain.Models.Base
{
    public class OrderBase
    {
        [JsonProperty("JrIdn")]
        public long JrId { get; set; }
        [JsonProperty("journalidn")]
        public long? JournalId { get; set; }
        [JsonProperty("object")]
        [ForeignKey("object")]
        public long Object { get; set; }
        [JsonProperty("docnumber")]
        public string? DocNumber { get; set; } = string.Empty;
        [JsonProperty("date")]
        public DateTime Date { get; set; }
        [JsonProperty("orderState")]
        public OrderState OrderState { get; set; } = OrderState.New;
        [JsonProperty("declineNote")]
        public string? DeclineNote { get; set; } = string.Empty;
        public string GetCleanDocNumber()
        {
            if (DocNumber == null || DocNumber == string.Empty) return string.Empty;
            var searchWord = "закупка";
            if (DocNumber.Contains("закупка".ToUpper()))
            {
                var position = searchWord.Length + 2;
                return DocNumber.Substring(position).Trim();
            }
            return string.Empty;
        }
    }
}