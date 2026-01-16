using Application.Abstractions.Data;
using Contract.Abstractions.Messages;
using Contract.Abstractions.Shared.Results;
using Contract.Services.Shipment.Share;
using Contract.Services.Shipment.ShipperUpdateStatus;
using Contract.Services.Shipment.UpdateStatus;
using Domain.Entities;
using Domain.Exceptions.Shipments;

namespace Application.UserCases.Commands.Shipments.ShipperUpdateStatus;

internal sealed class ShipperUpdateStatusCommandHandler(
    IShipmentRepository _shipmentRepository,
    IUnitOfWork _unitOfWork) : ICommandHandler<ShipperUpdateStatusCommand>
{
    public async Task<Result.Success> Handle(ShipperUpdateStatusCommand request, CancellationToken cancellationToken)
    {
        var updateRequest = request.updateStatus;

        var shipment = await GetAndValidateInput(updateRequest, request.Id, request.shipperId);

        shipment.UpdateStatus(request.shipperId, updateRequest.Status);

        _shipmentRepository.Update(shipment);
        await _unitOfWork.SaveChangesAsync();

        return Result.Success.Update();
    }

    private async Task<Shipment> GetAndValidateInput(UpdateStatusRequest updateRequest, Guid shipmentId, string shipperId)
    {
        if (shipmentId != updateRequest.ShipmentId)
        {
            throw new ShipmentIdConflictException();
        }

        if (!Enum.IsDefined(typeof(Status), updateRequest.Status))
        {
            throw new ShipmentStatusNotFoundException();
        }

        var shipment = await _shipmentRepository.GetShipmentDetailByIdAndShipperAsync(shipmentId, shipperId);
        if (shipment == null)
        {
            throw new ShipmentNotFoundException();
        }

        if (shipment.IsAccepted)
        {
            throw new ShipmentAlreadyDoneException();
        }

        return shipment;
    }
}