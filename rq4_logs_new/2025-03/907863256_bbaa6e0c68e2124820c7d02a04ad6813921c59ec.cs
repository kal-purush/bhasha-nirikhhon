using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CaptoneProject_IOTS_BOs.DTO.GHTKDTO
{
    public class ShippingFeeRequest
    {
        public string PickProvince { get; set; }
        public string PickDistrict { get; set; }
        public string Province { get; set; }
        public string District { get; set; }
        public string Address { get; set; }
        public int Weight { get; set; }
        public string deliver_option { get; set; }
    }
}