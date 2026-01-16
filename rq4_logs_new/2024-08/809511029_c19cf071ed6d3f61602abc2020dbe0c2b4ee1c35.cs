using Application.Paging;
using Application.Repositories;
using Mapster;
using MediatR;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Application.Queries
{
    public class GetPayments
    {
        public record Query(bool UsePaging) : PageRequest, IRequest<IReadOnlyList<PaymentResponse>>;
        public record PaymentResponse(string PaymentReference, decimal Amount, string Option);
        public class Handler(IPaymentRepository _paymentRepository) : IRequestHandler<Query, IReadOnlyList<PaymentResponse>>
        {
            public async Task<IReadOnlyList<PaymentResponse>> Handle(Query request, CancellationToken cancellationToken)
            {
                var invoices = await _paymentRepository.GetAllAsync(request, null, request.UsePaging);
                return invoices.Items.Adapt<IReadOnlyList<PaymentResponse>>();
            }
        }
    }
}