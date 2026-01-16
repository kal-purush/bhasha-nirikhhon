using Application.Abstractions.Data;
using AutoMapper;
using Contract.Abstractions.Messages;
using Contract.Abstractions.Shared.Results;
using Contract.Services.Product.SharedDto;
using Contract.Services.Set.GetSet;
using Contract.Services.Shipment.Share;
using Contract.Services.ShipOrder.GetShipOrderByOrderId;
using Contract.Services.ShipOrder.Share;
using Domain.Exceptions.ShipOrder;

namespace Application.UserCases.Queries.ShipOrders.GetShipOrderByOrderId;

internal sealed class GetShipOrderByOrderIdQueryHandler(IShipOrderRepository _shipOrderRepository, IMapper _mapper) 
    : IQueryHandler<GetShipOrderByOrderIdQuery, List<ShipOrderResponse>>
{
    public async Task<Result.Success<List<ShipOrderResponse>>> Handle(
        GetShipOrderByOrderIdQuery request,
        CancellationToken cancellationToken)
    {
        var shipOrders = await _shipOrderRepository.GetByOrderIdAsync(request.id);
        if(shipOrders == null)
        {
            return Result.Success<List<ShipOrderResponse>>.Get(null);
        }

        var shipOrderResponses = shipOrders.Select(shipOrder =>
        {
            if(shipOrder.ShipOrderDetails == null)
            {
                throw new ShipOrderDetailNotFoundException();
            }

            var shipOrderDetailResponses = shipOrder.ShipOrderDetails.Select(detail =>
            {
                var product = detail.Product != null ? _mapper.Map<ProductResponse>(detail.Product) : null;
                var set = detail.Set != null ? _mapper.Map<SetResponse>(detail.Set) : null;
                return new ShipOrderDetailResponse(product, set, detail.Quantity);
            }).ToList();

            return new ShipOrderResponse(
                shipOrder.ShipperId,
                shipOrder.Shipper.FirstName + " " + shipOrder.Shipper.LastName,
                shipOrder.ShipDate,
                shipOrder.Status,
                shipOrder.Status.GetDescription(),
                shipOrder.DeliveryMethod,
                shipOrder.DeliveryMethod.GetDescription(),
                shipOrderDetailResponses);
        }).ToList();

        return Result.Success<List<ShipOrderResponse>>.Get(shipOrderResponses);
    }
}