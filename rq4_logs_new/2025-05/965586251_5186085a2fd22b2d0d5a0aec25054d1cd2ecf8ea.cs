using DeliveryApp.Core.Domain.Services.DispatchService;
using DeliveryApp.Core.Ports;
using MediatR;
using Primitives;

namespace DeliveryApp.Core.Application.UseCases.Commands.AssignOrder;

public sealed class AssignOrderHandler(
    IOrderRepository orderRepository,
    ICourierRepository courierRepository,
    IDispatchService dispatchService,
    IUnitOfWork unitOfWork) : IRequestHandler<AssignOrderCommand, bool>
{
    public async Task<bool> Handle(AssignOrderCommand request, CancellationToken cancellationToken)
    {
        var order = await orderRepository.GetFirstCreatedAsync(cancellationToken);
        
        var couriers = await courierRepository.GetAllFreeAsync(cancellationToken);
        if (couriers.Count == 0)
        {
            throw new Exception("Не найден ни один свободный курьер");
        }
        
        var courier = dispatchService.Dispatch(order, couriers.ToList());
        
        orderRepository.Update(order);
        courierRepository.Update(courier);

        return await unitOfWork.SaveChangesAsync(cancellationToken);
    }
}