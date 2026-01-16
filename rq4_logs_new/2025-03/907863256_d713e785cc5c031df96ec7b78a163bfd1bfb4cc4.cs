using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CaptoneProject_IOTS_BOs.DTO.GHTKDTO
{
    public class ShipmentResponse
    {
        public int ShopOwnerId { get; set; } // ID của store (shop)
        public string Message { get; set; } // Thông báo kết quả
        public bool IsSuccess { get; set; } // Trạng thái thành công hay thất bại
        public string OrderId { get; set; } // Mã đơn hàng GHTK (nếu có)
    }
}