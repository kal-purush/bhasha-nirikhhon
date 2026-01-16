using System;
using System.Collections.Generic;

namespace AvSBookStore.Contractors
{
    public class CashPaymentService : IPaymentService
    {
        public string UniqCode => "Cash";

        public string Title => "Cash payment";

        public Form CreateForm(Order order)
        {
            return new Form(UniqCode, order.Id, 1, false, new Field[0]);
        }

        public OrderPayment GetPayment(Form form)
        {
            if (form.UniqCode != UniqCode || !form.IsFinal)
            {
                throw new InvalidOperationException("Inavlid payment form.");
            }

            return new OrderPayment(UniqCode, "Cash payment", new Dictionary<string, string>());
        }

        public Form MoveNext(int orderId, int step, IReadOnlyDictionary<string, string> values)
        {
            if (step != 1)
            {
                throw new InvalidOperationException("Invalid cash step.");
            }

            return new Form(UniqCode, orderId, 1, true, new Field[0]);
        }
    }
}