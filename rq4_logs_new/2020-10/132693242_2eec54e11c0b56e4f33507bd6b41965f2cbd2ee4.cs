using System;

namespace Com.DanLiris.Service.Purchasing.Lib.Services
{
    class APIDefaultResponse <T>
    {
        public T data { get; set; }
    }

    public class GarmentCurrency
    {
        public string UId { get; set; }
        public string Code { get; set; }
        public DateTime Date { get; set; }
        public double? Rate { get; set; }
    }
}