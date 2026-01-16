using Newtonsoft.Json;

namespace NikiConnectAPI.Lib.Models.Flyers
{
    public class FlyerItemDetail
    {
        [JsonProperty("object")]
        public string Object { get; set; }

        [JsonProperty("id")]
        public int Id { get; set; }

        [JsonProperty("flyer_id")]
        public int FlyerId { get; set; }

        [JsonProperty("barcode")]
        public string Barcode { get; set; }

        [JsonProperty("tax_included_price")]
        public double TaxIncludedPrice { get; set; }

        [JsonProperty("created_by")]
        public int CreatedBy { get; set; }

        [JsonProperty("updated_by")]
        public object UpdatedBy { get; set; }

        [JsonProperty("created_at")]
        public string CreatedAt { get; set; }

        [JsonProperty("updated_at")]
        public string UpdatedAt { get; set; }

        [JsonProperty("pimItem")]
        public PimItem PimItem { get; set; }
    }
}