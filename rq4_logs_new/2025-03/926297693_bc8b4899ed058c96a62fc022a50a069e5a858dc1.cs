using SPSS.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using VNPAY.NET.Models;

namespace SPSS.Service.Dto.Response
{
    public class PaymentResultResponse
    {
        public PaymentResult PaymentResult { get; set; }
        public OrderResponse Order { get; set; }
    }
}