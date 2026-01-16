using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CaptoneProject_IOTS_BOs.DTO.VNPayDTO
{
    public class VNPayRefundResponseDTO
    {
        public string Vnp_TmnCode { get; set; }      // Mã website của merchant
        public string Vnp_TxnRef { get; set; }       // Mã giao dịch tại merchant
        public string Vnp_TransactionNo { get; set; } // Mã giao dịch tại VNPAY
        public string Vnp_ResponseCode { get; set; } // Mã phản hồi của VNPay ("00" là thành công)
        public string Vnp_Message { get; set; }      // Mô tả lỗi (nếu có)
        public string Vnp_TransactionStatus { get; set; } // Trạng thái giao dịch
        public string Vnp_Amount { get; set; }       // Số tiền hoàn trả (VND)
        public string Vnp_OrderInfo { get; set; }    // Thông tin đơn hàng
        public string Vnp_PayDate { get; set; }      // Ngày thanh toán (yyyyMMddHHmmss)
        public string Vnp_SecureHash { get; set; }   // Chuỗi kiểm tra bảo mật
    }

}