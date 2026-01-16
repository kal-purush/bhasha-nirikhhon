using Application.Paging;
using Application.Repositories;
using Domain.Enums;
using Mapster;
using MediatR;

namespace Application.Queries
{
    public class GetInvoices
    {
        public record Query(bool UsePaging) : PageRequest, IRequest<IReadOnlyList<InvoiceResponse>>;
        public record InvoiceResponse(Guid Id, string JamaatName, string Reference, decimal Amount, InvoiceStatus Status);

        public class Handler(IInvoiceRepository _invoiceRepository) : IRequestHandler<Query, IReadOnlyList<InvoiceResponse>>
        {
            public async Task<IReadOnlyList<InvoiceResponse>> Handle(Query request, CancellationToken cancellationToken)
            {
                var invoices = await _invoiceRepository.GetAllAsync(request, null, request.UsePaging);
                return invoices.Items.Adapt<IReadOnlyList<InvoiceResponse>>();
            }
        }
    }
}