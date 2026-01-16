using Application.Contracts;
using Domain.Enums;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Infrastructure.Services.PaymentServices
{
    public class PaymentGatewayFactory: IPaymentGatewayFactory
    {
        private readonly IServiceProvider _serviceProvider;

        public PaymentGatewayFactory(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;
        }

        public IPaymentService GetPaymentService(PaymentOption option)
        {
            return option switch
            {
                PaymentOption.Paystack => _serviceProvider.GetRequiredService<PaystackService>(),
                PaymentOption.VirtualAccount => _serviceProvider.GetRequiredService<VirtualPaymentService>(),
                PaymentOption.Wallet => _serviceProvider.GetRequiredService<WalletPaymentService>(),
                _ => throw new NotSupportedException("Unsupported payment gateway type")
            };
        }
    }
}