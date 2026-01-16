using Application.DTOs;
using Application.Exceptions;
using Application.Helpers;
using Application.Repositories;
using Domain.Entities;
using Domain.Enums;
using Domain.Exceptions;
using FluentValidation;
using Mapster;
using MediatR;
using static Application.Queries.GetInvoice;

namespace Application.Commands
{
    public class CreateGroupInvoice
    {
        public record Command : IRequest<InvoiceResponse>
        {
            public InvoiceType Type { get; init; }
            public IReadOnlyList<InvoiceItemCommand> InvoiceItems { get; init; } = new List<InvoiceItemCommand>();
        }
        public record InvoiceItemCommand
        {
            public string PayerNo { get; init; } = default!;
            public string ReceiptNo { get; init; } = default!;
            public MonthOfTheYear MonthPaidFor { get; init; }
            public int Year { get; init; }
            public IReadOnlyList<ChandaItemCommand> ChandaItems { get; init; } = new List<ChandaItemCommand>();
        }
        public record ChandaItemCommand(string ChandaTypeCode, decimal Amount);

        public class Handler : IRequestHandler<Command, InvoiceResponse>
        {
            private readonly IUnitOfWork _unitOfWork;
            private readonly ICurrentUser _currentUser;
            private readonly IMemberRepository _memberRepository;
            private readonly IInvoiceRepository _invoiceRepository;
            private readonly IChandaTypeRepository _chandaTypeRepository;

            public Handler(IUnitOfWork unitOfWork, ICurrentUser currentUser, IMemberRepository memberRepository, IInvoiceRepository invoiceRepository, IChandaTypeRepository chandaTypeRepository)
            {
                _unitOfWork = unitOfWork;
                _currentUser = currentUser;
                _memberRepository = memberRepository;
                _invoiceRepository = invoiceRepository;
                _chandaTypeRepository = chandaTypeRepository;
            }

            public async Task<InvoiceResponse> Handle(Command request, CancellationToken cancellationToken)
            {
                var invoiceAmount = 0.0m;
                var invoiceId = Guid.NewGuid();
                var reference = Utility.GenerateReference(request.Type.ToString());

                var initiator = new MemberDetials { Name = "Ade Ola", ChandaNo = "0001", Email = "adeola@example.com", JamaatId = new Guid("5f9013da-5c0a-4af0-ae51-738f6bc0009d"), Role = "Jamaat-President" };//_currentUser.GetMemberDetails();
                
                if (initiator is null || string.IsNullOrEmpty(initiator.ChandaNo))
                {
                    throw new NotFoundException($"Please login to create an invoice.", ExceptionCodes.InvalidJamaat.ToString(), 403);
                }

                var chandaTypeCodes = request.InvoiceItems.SelectMany(ii => ii.ChandaItems.Select(ci => ci.ChandaTypeCode)).ToList();
                var validChandaTypes = _chandaTypeRepository.GetChandaTypes(chandaTypeCodes);

                if (validChandaTypes is null || !validChandaTypes.Any())
                {
                    throw new NotFoundException("No valid ChandaType selected", ExceptionCodes.NoValidChandaTypeSelected.ToString(), 400);
                }

                var payerNos = request.InvoiceItems.Select(ii => ii.PayerNo).ToList();
                var payers = _memberRepository.GetMembers(m => payerNos.Contains(m.ChandaNo)).ToList();

                var invoiceItems = new List<InvoiceItem>();
                foreach (var item in request.InvoiceItems)
                {
                    var payer = payers.Where(p => p.ChandaNo == item.PayerNo).FirstOrDefault();
                    if (payer is not null)
                    {
                        var invoiceItemAmount = 0.0m;
                        var invoiceItemId = Guid.NewGuid();
                        var chandaItems = new List<ChandaItem>();
                        foreach (var chanda in item.ChandaItems)
                        {
                            var chandaType = validChandaTypes.Where(ct => ct.Code.Equals(chanda.ChandaTypeCode, StringComparison.OrdinalIgnoreCase)).FirstOrDefault();
                            if (chandaType is not null)
                            {
                                var chandaItem = new ChandaItem(invoiceItemId, chandaType.Id, chanda.Amount, initiator.ChandaNo);
                                chandaItems.Add(chandaItem);
                                invoiceItemAmount += chanda.Amount;
                            }
                        }

                        if (chandaItems.Any())
                        {
                            var invoiceItem = new InvoiceItem(invoiceItemId, payer.Id, invoiceId, item.ReceiptNo, invoiceItemAmount, item.MonthPaidFor, item.Year, initiator.ChandaNo);
                            invoiceItem.AddChandaItems(chandaItems);
                            invoiceAmount += invoiceItemAmount;
                            invoiceItems.Add(invoiceItem);
                        }
                    }
                }
                if (!invoiceItems.Any())
                {
                    throw new NotFoundException("No Valid Item selected", ExceptionCodes.NoInvoiceItemSelected.ToString(), 400);
                }

                var invoice = new Invoice(invoiceId, initiator.JamaatId, reference, invoiceAmount, InvoiceStatus.Pending, initiator.ChandaNo);
                invoice.AddInvoiceItems(invoiceItems);

                invoice = await _invoiceRepository.AddAsync(invoice);
                await _unitOfWork.SaveChangesAsync(cancellationToken);

                return invoice.Adapt<InvoiceResponse>();
            }
        }

        public class CommandValidator : AbstractValidator<Command>
        {
            public CommandValidator()
            {
                RuleFor(command => command.InvoiceItems)
                .NotNull().WithMessage("Invoice items cannot be null.")
                .NotEmpty().WithMessage("No Invoice Item added.");

                    RuleForEach(command => command.InvoiceItems).ChildRules(invoiceItem =>
                    {
                        invoiceItem.RuleFor(i => i.PayerNo)
                            .NotEmpty().WithMessage("PayerNo is required.");

                        invoiceItem.RuleFor(i => i.MonthPaidFor)
                            .IsInEnum().WithMessage("Invalid MonthPaidFor value.");

                        invoiceItem.RuleFor(i => i.Year)
                            .InclusiveBetween(2000, 2100)
                            .WithMessage("Year must be between 2000 and 2100.");

                        invoiceItem.RuleFor(i => i.ChandaItems)
                            .NotNull().WithMessage("Chanda items cannot be null.")
                            .NotEmpty().WithMessage("Chanda Items cannot be empty.")
                            .Must(chandaItems =>
                                chandaItems != null && chandaItems.GroupBy(ci => ci.ChandaTypeCode).All(g => g.Count() == 1))
                            .WithMessage("Chanda items cannot have duplicate ChandaTypeCode.");

                        invoiceItem.RuleForEach(i => i.ChandaItems).ChildRules(chandaItem =>
                        {
                            chandaItem.RuleFor(ci => ci.ChandaTypeCode)
                                .NotEmpty().WithMessage("ChandaTypeCode is required.");

                            chandaItem.RuleFor(ci => ci.Amount)
                                .GreaterThan(0).WithMessage("Invalid Amount inputed.");
                        });
                    });

                RuleFor(command => command.InvoiceItems)
                    .Must(invoiceItems =>
                        invoiceItems != null && invoiceItems.GroupBy(i => i.ReceiptNo)
                            .All(g => g.Count() == 1))
                    .WithMessage("Two InvoiceItems should not have the same ReceiptNo.");

                RuleFor(command => command.InvoiceItems)
                    .Must(invoiceItems =>
                        invoiceItems != null && invoiceItems.GroupBy(i => new { i.PayerNo, i.MonthPaidFor })
                            .All(g => g.Count() == 1))
                    .WithMessage("Two InvoiceItems should not have the same PayerNo and MonthPaidFor together.");
            }
        }
    }
}