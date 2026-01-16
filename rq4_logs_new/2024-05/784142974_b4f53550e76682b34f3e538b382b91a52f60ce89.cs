using DeliveryApp.Core.Domain.CourierAggregate;
using DeliveryApp.Core.Domain.OrderAggregate;

namespace DeliveryApp.Core.DomainServices.Dispatch;

public class DispatchService : IDispatchService
{
    public void Dispatch(Order order, IReadOnlyList<Courier> couriers)
    {
        var bestCourier = couriers
                          .Where(x => x.CanHandle(order.Weight))
                          .MinBy(x => x.CalculateTime(order.DeliveryLocation));

        if (bestCourier == null)
        {
            throw Exceptions.NoCouriersFound(order.Id);
        }
        
        bestCourier.AcceptOrder(order.Id);
        order.AssignTo(bestCourier.Id);
    }
}