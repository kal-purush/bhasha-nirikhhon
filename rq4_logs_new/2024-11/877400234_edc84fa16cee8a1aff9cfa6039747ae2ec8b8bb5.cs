using System;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace NikiConnectAPI.Lib.Models.Flyers
{
    public class Flyer
    {
        [JsonProperty("object")]
        public string Object { get; set; }

        [JsonProperty("id")]
        public int Id { get; set; }

        [JsonProperty("matchcode")]
        public string Matchcode { get; set; }

        [JsonProperty("company_id")]
        public int CompanyId { get; set; }

        [JsonProperty("status_id")]
        public int StatusId { get; set; }

        [JsonProperty("group_id")]
        public int GroupId { get; set; }

        [JsonProperty("typeable_type_id")]
        public int TypeableTypeId { get; set; }

        [JsonProperty("attachment_id")]
        public object AttachmentId { get; set; }

        [JsonProperty("barcodesAndPrices")]
        public List<BarcodesAndPrice> BarcodesAndPrices { get; set; }

        [JsonProperty("start_at")]
        public DateTime StartAt { get; set; }

        [JsonProperty("finish_at")]
        public DateTime FinishAt { get; set; }

        [JsonProperty("created_by")]
        public int CreatedBy { get; set; }

        [JsonProperty("updated_by")]
        public object UpdatedBy { get; set; }

        [JsonProperty("created_at")]
        public DateTime CreatedAt { get; set; }

        [JsonProperty("updated_at")]
        public DateTime UpdatedAt { get; set; }
    }
}