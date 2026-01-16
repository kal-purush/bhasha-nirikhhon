using CaptoneProject_IOTS_BOs.DTO.ProductDTO;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CaptoneProject_IOTS_BOs.DTO.GHTKDTO
{
    public class TrackingResponse
    {
        [JsonProperty("label_id")]
        public string LabelId { get; set; }

        [JsonProperty("partner_id")]
        public string PartnerId { get; set; }

        [JsonProperty("order_id")]
        public string OrderId { get; set; }

        public int Status { get; set; }

        [JsonProperty("status_text")]
        public string StatusText { get; set; }

        public string Created { get; set; }
        public string Modified { get; set; }
        public string Message { get; set; }

        [JsonProperty("pick_date")]
        public string PickDate { get; set; }

        [JsonProperty("deliver_date")]
        public string DeliverDate { get; set; }

        [JsonProperty("customer_fullname")]
        public string CustomerFullname { get; set; }

        [JsonProperty("customer_tel")]
        public string CustomerTel { get; set; }

        public string Address { get; set; }

        [JsonProperty("storage_day")]
        public int StorageDay { get; set; }

        [JsonProperty("ship_money")]
        public int ShipMoney { get; set; }

        public int Insurance { get; set; }
        public int Value { get; set; }
        public int Weight { get; set; }

        [JsonProperty("pick_money")]
        public int PickMoney { get; set; }

        [JsonProperty("is_freeship")]
        public int IsFreeship { get; set; }

        [JsonProperty("products")]
        public List<ProductTracking> Products { get; set; }
    }

    public class ProductTracking
    {
        [JsonProperty("full_name")]
        public string Name { get; set; }

        [JsonProperty("product_code")]
        public string Code { get; set; }

        [JsonProperty("weight")]
        public string Weight { get; set; }

        [JsonProperty("quantity")]
        public string Quantity { get; set; }

    }


    public class TrackingResponseWrapper
    {
        public bool Success { get; set; }
        public string Message { get; set; }
        public TrackingResponse Order { get; set; }
    }
}