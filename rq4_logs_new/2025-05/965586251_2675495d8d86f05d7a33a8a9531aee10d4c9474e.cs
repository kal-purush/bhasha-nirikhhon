using DeliveryApp.Core.Domain.Models.CourierAggregate;
using DeliveryApp.Core.Domain.Models.OrderAggregate;

namespace DeliveryApp.Core.Domain.Services.DispatchService;

public class DispatchService : IDispatchService
{
    public void Dispatch(Order order, List<Courier> couriers)
    {
        if (order.Status != OrderStatus.Created) throw new Exception($"Невозможно назначить заказ в статусе \"{order.Status}\"");
        
        var availableCouriers = couriers.Where(x => x.Status == CourierStatus.Free).ToList();
        if (!availableCouriers.Any()) throw new Exception("Нет ни одного доступного курьера");
        
        var resultCourier = availableCouriers.First();
        var resultTime = resultCourier.CalculateTimeToLocation(order.Location);

        foreach (var courier in couriers)
        {
            if (resultCourier.Id != courier.Id)
            {
                var time = courier.CalculateTimeToLocation(order.Location);
                if (time < resultTime)
                {
                    resultCourier = courier;
                    resultTime = time;
                }
            }
        }
        
        order.Assign(resultCourier.Id);
    }
}