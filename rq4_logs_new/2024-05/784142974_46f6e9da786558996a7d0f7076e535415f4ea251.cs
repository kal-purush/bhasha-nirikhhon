using DeliveryApp.Core.Domain.CourierAggregate;
using DeliveryApp.Core.Domain.OrderAggregate;
using DeliveryApp.Core.DomainServices.Dispatch;
using MediatR;
using Primitives;

namespace DeliveryApp.Core.Application.Commands.Couriers.AssignOrder;

public class Handler : IRequestHandler<Command, bool>
{
    private readonly IUnitOfWork _unitOfWork;
    private readonly IOrderRepository _orderRepository;
    private readonly ICourierRepository _courierRepository;
    private readonly IDispatchService _dispatchService;
    private readonly GetAllNotAssignedOrders _getAllNotAssignedOrders;
    private readonly GetAllReadyCouriers _getAllReadyCouriers;

    public Handler(
        IUnitOfWork unitOfWork,
        IOrderRepository orderRepository,
        ICourierRepository courierRepository,
        IDispatchService dispatchService,
        GetAllNotAssignedOrders getAllNotAssignedOrders,
        GetAllReadyCouriers getAllReadyCouriers)
    {
        _unitOfWork = unitOfWork;
        _orderRepository = orderRepository;
        _courierRepository = courierRepository;
        _dispatchService = dispatchService;
        _getAllNotAssignedOrders = getAllNotAssignedOrders;
        _getAllReadyCouriers = getAllReadyCouriers;
    }

    public async Task<bool> Handle(Command request, CancellationToken cancellationToken)
    {
        var order = await GetFirstUnassignedOrder();
        if (order == null)
            return false;

        var readyCouriers = await GetReadyCouriers();
        if (readyCouriers.Count == 0)
            return false;

        var dispatchResult = _dispatchService.Dispatch(order, readyCouriers);
        if (dispatchResult.IsFailure) return false;

        var courier = dispatchResult.Value;
        
        _courierRepository.Update(courier); // Change tracking?
        _orderRepository.Update(order);
        return await _unitOfWork.SaveEntitiesAsync(cancellationToken);
    }

    private async Task<IReadOnlyCollection<Courier>> GetReadyCouriers()
    {
        var readyCourierIds = await _getAllReadyCouriers();
        if (readyCourierIds.Count == 0) return Array.Empty<Courier>();
        
        var readyCouriers = new List<Courier>();
        foreach (var courierId in readyCourierIds)
        {
            var readyCourier = await _courierRepository.Get(courierId) ?? throw new InvalidOperationException();
            readyCouriers.Add(readyCourier!);
        }

        return readyCouriers;
    }

    private async Task<Order?> GetFirstUnassignedOrder()
    {
        var unassignedOrders = await _getAllNotAssignedOrders();
        if (unassignedOrders.Count == 0) return null;
        
        Order order = await _orderRepository.Get(unassignedOrders.First()) 
                      ?? throw new InvalidOperationException();
        return order;
    }
}