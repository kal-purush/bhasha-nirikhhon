using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CaptoneProject_IOTS_BOs.DTO.OrderDTO
{
    public class OrderResponseDTO
    {
        public string ApplicationSerialNumber { set; get; }

        public decimal TotalPrice { set; get; } = 0;

        public string Address { set; get; } = "";

        public string ContactNumber { set; get; } = "";

        public string Notes { set; get; }

        public DateTime CreateDate { set; get; }

        public DateTime UpdatedDate { set; get; }

        public string OrderStatusString { set; get; }
    }
}