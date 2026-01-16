using Application.Abstractions.Data;
using Contract.Abstractions.Messages;
using Contract.Abstractions.Shared.Results;
using Contract.Services.Shipment.Share;
using Contract.Services.Shipment.UpdateAccepted;
using Contract.Services.ShipmentDetail.Share;
using Domain.Entities;
using Domain.Exceptions.Materials;
using Domain.Exceptions.ProductPhases;
using Domain.Exceptions.ShipmentDetails;
using Domain.Exceptions.Shipments;
using MediatR;

namespace Application.UserCases.Commands.Shipments.UpdateAccepted;

internal sealed class UpdateAcceptedCommandHandler(
    IShipmentRepository _shipmentRepository,
    ICompanyRepository _companyRepository,
    IMaterialRepository _materialRepository,
    IProductPhaseRepository _productPhaseRepository,
    IUnitOfWork _unitOfWork) : ICommandHandler<UpdateAcceptedCommand>
{
    public async Task<Result.Success> Handle(UpdateAcceptedCommand request, CancellationToken cancellationToken)
    {
        var shipment = await GetAndValidateInput(request);

        await UpdateQuantityOfProductPhaseAndMaterial(shipment);

        shipment.UpdateAccepted(request.updatedBy);

        _shipmentRepository.Update(shipment);
        await _unitOfWork.SaveChangesAsync();   

        return Result.Success.Update();
    }

    private async Task<Shipment> GetAndValidateInput(UpdateAcceptedCommand request)
    {
        var shipment = await _shipmentRepository.GetByIdAndShipmentDetailAsync(request.shipmentId)
            ?? throw new ShipmentNotFoundException();

        if (shipment.Status == Status.WAIT_FOR_SHIP || shipment.Status == Status.SHIPPING)
        {
            throw new ShipmentBadRequestException("Chỉ chốt được đơn hàng khi đơn hàng đã hoàn thành hoặc đã bị hủy");
        }

        if (shipment.IsAccepted)
        {
            throw new ShipmentBadRequestException("Đơn hàng đã được chốt");
        }

        return shipment;
    }

    private async Task UpdateQuantityOfProductPhaseAndMaterial(Shipment shipment)
    {
        var shipmentDetails = shipment.ShipmentDetails ?? throw new ShipmentDetailNotFoundException();

        var isFromCompanyThirdParty = await _companyRepository.IsThirdPartyCompanyAsync(shipment.FromId);

        if(isFromCompanyThirdParty)
        {
            await UpdateQuantityOfProductPhaseWhenSendFromThirdPartyCompany(shipmentDetails, shipment.ToId, shipment.FromId);
        }
        else
        {
            await UpdateQuantityOfProductPhaseWhenSendFromFactory(shipmentDetails, shipment.ToId, shipment.FromId);
        }

        await UpdateQuantityOfMaterial(shipmentDetails, isFromCompanyThirdParty);
    }


    private async Task UpdateQuantityOfProductPhaseWhenSendFromFactory(List<ShipmentDetail> shipmentDetails, Guid toId, Guid fromId)
    {
        var _productsPhases = new List<ProductPhase>();

        foreach (var detail in shipmentDetails)
        {
            if (detail.ProductId != null && detail.PhaseId != null)
            {
                var productPhaseFromCompany = _productsPhases
                    .Where(p => p.PhaseId == detail.PhaseId && p.CompanyId == fromId && p.ProductId == detail.ProductId)
                    .FirstOrDefault();

                if (productPhaseFromCompany == null)
                {
                    productPhaseFromCompany = await _productPhaseRepository.GetByProductIdPhaseIdAndCompanyIdAsync(
                        (Guid) detail.ProductId, 
                        (Guid) detail.PhaseId, 
                        fromId) ?? throw new ProductPhaseNotFoundException();

                    _productsPhases.Add(productPhaseFromCompany);
                }

                var productPhaseToCompany = _productsPhases
                    .Where(p => p.PhaseId == detail.PhaseId && p.CompanyId == fromId && p.ProductId == detail.ProductId)
                    .FirstOrDefault();

                if (productPhaseToCompany == null)
                {
                    productPhaseToCompany = await _productPhaseRepository.GetByProductIdPhaseIdAndCompanyIdAsync(
                        (Guid)detail.ProductId,
                        (Guid)detail.PhaseId,
                        toId) ?? throw new ProductPhaseNotFoundException();

                    _productsPhases.Add(productPhaseToCompany);
                }

                if (detail.ProductPhaseType == ProductPhaseType.NO_PROBLEM)
                {
                    var quantityOfFromCompany = productPhaseFromCompany.Quantity - (int)detail.Quantity;
                    if (quantityOfFromCompany < 0) throw new ItemAvailableNotEnoughException();
                    productPhaseFromCompany.UpdateQuantity(quantityOfFromCompany);

                    productPhaseToCompany.UpdateQuantity(productPhaseToCompany.Quantity + (int) detail.Quantity);
                }
                else if(detail.ProductPhaseType == ProductPhaseType.THIRD_PARTY_ERROR)
                {
                    var quantityOfFromCompany = productPhaseFromCompany.ErrorQuantity - (int)detail.Quantity;
                    if (quantityOfFromCompany < 0) throw new ItemAvailableNotEnoughException();
                    productPhaseFromCompany.UpdateErrorAvailableQuantity(quantityOfFromCompany);

                    productPhaseToCompany.UpdateQuantity(productPhaseToCompany.ErrorQuantity + (int)detail.Quantity);
                }
                else
                {
                    throw new ShipmentBadRequestException("Cơ sở chỉ được gửi sản phẩm bình thường và sản phẩm lỗ do bên thứ 3");
                }
                
            }
        }

        _productPhaseRepository.UpdateProductPhaseRange(_productsPhases);
    }

    private async Task UpdateQuantityOfProductPhaseWhenSendFromThirdPartyCompany(List<ShipmentDetail> shipmentDetails, Guid toId, Guid fromId)
    {
        var _productsPhases = new List<ProductPhase>();

        foreach (var detail in shipmentDetails)
        {
            if (detail.ProductId != null && detail.PhaseId != null)
            {
                var productPhasesFromCompany = _productsPhases
                    .Where(p => p.CompanyId == fromId && p.ProductId == detail.ProductId)
                    .ToList();

                if (productPhasesFromCompany == null)
                {
                    productPhasesFromCompany = await _productPhaseRepository.GetByProductIdAndCompanyIdAsync(
                        (Guid)detail.ProductId,
                        fromId);

                    if(productPhasesFromCompany is null || productPhasesFromCompany.Count  == 0)
                    {
                        throw new ProductPhaseNotFoundException();
                    }

                    foreach (var p in productPhasesFromCompany)
                    {
                        _productsPhases.Add(p);
                    }
                }

                var productPhaseToCompany = _productsPhases
                    .Where(p => p.PhaseId == detail.PhaseId && p.CompanyId == fromId && p.ProductId == detail.ProductId)
                    .FirstOrDefault();

                if (productPhaseToCompany == null)
                {
                    productPhaseToCompany = await _productPhaseRepository.GetByProductIdPhaseIdAndCompanyIdAsync(
                        (Guid)detail.ProductId,
                        (Guid)detail.PhaseId,
                        toId) ?? throw new ProductPhaseNotFoundException();

                    _productsPhases.Add(productPhaseToCompany);
                }

                //chưa xong


            }
        }

        _productPhaseRepository.UpdateProductPhaseRange(_productsPhases);
    }

    private async Task UpdateQuantityOfMaterial(List<ShipmentDetail> shipmentDetails, bool isFromCompanyThirdParty)
    {
        var materialIds = shipmentDetails
            .Where(s => s.MaterialId != null)
            .Select(s => (Guid) s.MaterialId)
            .ToList();

        if (isFromCompanyThirdParty && materialIds is not null && materialIds.Count > 0)
        {
            throw new ShipmentBadRequestException("Công ty bên thứ 3 không gửi được nguyên liệu");
        }

        if(materialIds is null || materialIds.Count == 0)
        {
            return;
        }

        var materials = await _materialRepository.GetMaterialsByIdsAsync(materialIds);

        foreach(var detail in  shipmentDetails)
        {
            if(detail.MaterialId is not null)
            {
                var material = materials.SingleOrDefault(m => m.Id == detail.MaterialId)
                    ?? throw new MaterialNotFoundException();

                var quantity = material.QuantityInStock - detail.Quantity;

                if (quantity < 0) throw new ItemAvailableNotEnoughException();

                material.UpdateQuantityInStock(quantity);
            }
        }

        _materialRepository.UpdateRange(materials);
    }
}