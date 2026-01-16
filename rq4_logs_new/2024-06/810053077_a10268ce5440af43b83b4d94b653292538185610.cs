using backend_carhelp.shared.Domain.Repositories;
using backend_carhelp.Workshop_management.Domain.Model.Aggregates;
using backend_carhelp.Workshop_management.Domain.Model.Commands;
using backend_carhelp.Workshop_management.Domain.Repositories;
using backend_carhelp.Workshop_management.Domain.Services;

namespace backend_carhelp.Workshop_management.Application.Internal.CommandServices;

public class InvoiceCommandService(IInvoiceRepository invoiceRepository, IUnitOfWork unitOfWork) : IInvoiceCommandService
{
    public async Task<Invoice?> Handle(CreateInvoiceCommand command)
    {
        var invoice = new Invoice(command);
        try
        {
            await invoiceRepository.AddAsync(invoice);
            await unitOfWork.CompleteAsync();
            return invoice;
        } catch (Exception e)
        {
            Console.WriteLine($"An error occurred while creating the invoice: {e.Message}");
            return null;
        }
    }
}