using SPSS.Repository.Enum;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SPSS.Service.Dto.Response
{
    public class OrderFullResponse
    {
        public int Id { get; set; }
        public int? PromotionId { get; set; }
        public string UserName { get; set; }
        public OrderStatus Status { get; set; }
        public bool IsDelete { get; set; }
        public decimal TotalAmount { get; set; }
        public decimal OriginalTotalAmount { get; set; }
        public int ItemsCount { get; set; }
        public DateTime CreatedAt { get; set; }
        public DateTime? PaymentDate { get; set; }
        public DateTime? CompletedDate { get; set; }
        public DateTime? CanceledDate { get; set; }
        public List<OrderItemResponse> OrderItems { get; set; } = new();
    }

}