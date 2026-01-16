using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CaptoneProject_IOTS_BOs.DTO.GHTKDTO
{
    public class ShippingFeeResponse
    {
        public int ShopOwnerId { get; set; }

        [JsonProperty("fee")]
        public int Fee { get; set; }

        [JsonProperty("insurance_fee")]
        public int InsuranceFee { get; set; }

        [JsonProperty("ship_fee_only")]
        public int ShipFeeOnly { get; set; }

        public string Message { get; set; }
    }

    public class GHTKResponse
    {
        [JsonProperty("success")]
        public bool Success { get; set; }

        [JsonProperty("message")]
        public string Message { get; set; }

        [JsonProperty("fee")]
        public FeeData Fee { get; set; }

    }

    public class FeeData
    {
        [JsonProperty("fee")]
        public int Fee { get; set; }

        [JsonProperty("insurance_fee")]
        public int InsuranceFee { get; set; }

        [JsonProperty("ship_fee_only")]
        public int ShipFeeOnly { get; set; }
    }
}