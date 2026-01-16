using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace CaptoneProject_IOTS_BOs.DTO.AddressDTO
{
    public class LocationDataDTO
    {
        [JsonProperty("success")]
        public bool Success { get; set; }

        [JsonProperty("data")]
        public List<ApiProvince> Data { get; set; }
    }

    public class GHTKProvinceData
    {
        [JsonProperty("PROVINCE_ID")]
        public int PROVINCE_ID { get; set; }

        [JsonProperty("PROVINCE_NAME")]
        public string PROVINCE_NAME { get; set; }
    }

    public class GHTKDistrictResponse
    {
        public bool success { get; set; }
        public List<ApiDistrict> data { get; set; }
    }

    public class GHTKWardResponse
    {
        public bool success { get; set; }
        public List<ApiWard> data { get; set; }
    }

    public class GHTKAddressResponse
    {
        public bool success { get; set; }
        public List<ApiAddress> data { get; set; }
    }
}