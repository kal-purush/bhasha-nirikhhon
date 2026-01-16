using System.Collections.Generic;
using Sandwicherie.Model;

namespace Sandwicherie.Service;

public class Context
{
    public Strategy strategy { get; set; }

    public void executeStrategy(Invoice invoice)
    {
        strategy.execute(invoice);
    }

}