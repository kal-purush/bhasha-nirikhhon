using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PriorityQueue.Core
{
    public class InvoiceRepository
    {
        private readonly IQueryable<Invoice> _invoices;

        public InvoiceRepository(IQueryable<Invoice> invoices)
        {
            _invoices = invoices ?? throw new ArgumentNullException(nameof(invoices));
            // Console.WriteLine("Sample debug output");
        }

        /// <summary>
        /// Should return a total value of an invoice with a given id. If an invoice does not exist null should be returned.
        /// </summary>
        /// <param name="invoiceId"></param>
        /// <returns></returns>
        public decimal? GetTotal(int invoiceId)
        {
            var invoice = _invoices.FirstOrDefault(invoice => invoice.Id == invoiceId);

            if (invoice == null) return null;
                
            return invoice.InvoiceItems
                .Sum(invoiceItem => invoiceItem.Price * invoiceItem.Count);
        }

        /// <summary>
        /// Should return a total value of all unpaid invoices.
        /// </summary>
        /// <returns></returns>
        public decimal GetTotalOfUnpaid()
        {
            return _invoices.Where(invoice => invoice.AcceptanceDate == null)
                .SelectMany(invoice => invoice.InvoiceItems)
                .Sum(invoiceItem => invoiceItem.Price * invoiceItem.Count);
        }

        /// <summary>
        /// Should return a dictionary where the name of an invoice item is a key and the number of bought items is a value.
        /// The number of bought items should be summed within a given period of time (from, to). Both the from date and the end date can be null.
        /// </summary>
        /// <param name="from"></param>
        /// <param name="to"></param>
        /// <returns></returns>
        public IReadOnlyDictionary<string, long> GetItemsReport(DateTime? from, DateTime? to)
        {
            var itemsReport = new Dictionary<string, long>();
            var invoicesWithinRange = new List<Invoice>().AsQueryable();

            // Get the invoices that match the from and to criteria
            if (from.HasValue && to.HasValue)
            {
                invoicesWithinRange = _invoices.Where(invoice => invoice.CreationDate <= to.Value
                    && invoice.CreationDate >= from.Value);
            }
            else if (from.HasValue && to == null)
            {
                invoicesWithinRange = _invoices.Where(invoice => invoice.CreationDate >= from.Value);
            }
            else if (from == null && to.HasValue)
            {
                invoicesWithinRange = _invoices.Where(invoice => invoice.CreationDate <= to.Value);
            }
            else
            {
                invoicesWithinRange = _invoices;
            }

            // Iterate through the invoices and add the invoice item's name and count
            foreach (var invoice in invoicesWithinRange)
            {
                foreach (var invoiceItem in invoice.InvoiceItems)
                {
                    if (itemsReport.ContainsKey(invoiceItem.Name))
                    {
                        itemsReport[invoiceItem.Name] += invoiceItem.Count;
                    }
                    else
                    {
                        itemsReport.Add(invoiceItem.Name, invoiceItem.Count);
                    }
                }
            }

            return itemsReport;
        }
    }

    public class Invoice
    {
        // A unique numerical identifier of an invoice (mandatory)
        public int Id { get; set; }
        // A short description of an invoice (optional).
        public string Description { get; set; }
        // A number of an invoice e.g. 134/10/2018 (mandatory).
        public string Number { get; set; }
        // An issuer of an invoice e.g. Metz-Anderson, 600  Hickman Street,Illinois (mandatory).
        public string Seller { get; set; }
        // A buyer of a service or a product e.g. John Smith, 4285  Deercove Drive, Dallas (mandatory).
        public string Buyer { get; set; }
        // A date when an invoice was issued (mandatory).
        public DateTime CreationDate { get; set; }
        // A date when an invoice was paid (optional).
        public DateTime? AcceptanceDate { get; set; }
        // A collection of invoice items for a given invoice (can be empty but is never null).
        public IList<InvoiceItem> InvoiceItems { get; }

        public Invoice()
        {
            InvoiceItems = new List<InvoiceItem>();
        }
    }

    public class InvoiceItem
    {
        // A name of an item e.g. eggs.
        public string Name { get; set; }
        // A number of bought items e.g. 10.
        public uint Count { get; set; }
        // A price of an item e.g. 20.5.
        public decimal Price { get; set; }
    }
}