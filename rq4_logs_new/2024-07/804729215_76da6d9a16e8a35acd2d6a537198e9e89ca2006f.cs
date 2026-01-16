using Application.Abstractions.Data;
using Contract.Abstractions.Messages;
using Contract.Abstractions.Shared.Results;
using Contract.Services.ProductPhase.Creates;
using Contract.Services.ProductPhase.Updates;
using Domain.Abstractions.Exceptions;
using Domain.Entities;
using FluentValidation;

namespace Application.UserCases.Commands.ProductPhases.Updates;

public class UpdateQuantityPhaseCommnadHandler
    (IPhaseRepository _phaseRepository,
    IProductPhaseRepository _productPhaseRepository,
    ICompanyRepository _companyRepository,
    IValidator<UpdateQuantityPhaseRequest> _validator,
    IUnitOfWork _unitOfWork)
     : ICommandHandler<UpdateQuantityPhaseCommand>
{
    public async Task<Result.Success> Handle(UpdateQuantityPhaseCommand request, CancellationToken cancellationToken)
    {
        var validator = await _validator.ValidateAsync(request.updateReq, cancellationToken);
        if (!validator.IsValid)
        {
            throw new MyValidationException(validator.ToDictionary());
        }
        var phase = await _phaseRepository.GetPhaseByName("PH_003");
        var phaseId = phase.Id;

        var productPhase2 = await _productPhaseRepository.GetByProductIdPhaseIdCompanyID(request.updateReq.ProductId, request.updateReq.PhaseId, request.updateReq.CompanyId);
        var productPhase3 = await _productPhaseRepository.GetByProductIdPhaseIdCompanyID(request.updateReq.ProductId, phaseId, request.updateReq.CompanyId);


        var quantityAvailable = productPhase2.AvailableQuantity;
        if (quantityAvailable < request.updateReq.quantity)
        {
            throw new MyValidationException("Số lượng hàng hoàn thành lớn hơn số lượng hàng trong kho.");
        }

        var updateQuantityPhase2 = productPhase2.Quantity - request.updateReq.quantity;
        var updateAvailableQuantityPhase2 = productPhase2.AvailableQuantity - request.updateReq.quantity;
        productPhase2.UpdateQuantityPhase(updateQuantityPhase2, updateAvailableQuantityPhase2);
        _productPhaseRepository.UpdateProductPhase(productPhase2);

        if (productPhase3 == null)
        {
            var productPhase = ProductPhase.Create(new CreateProductPhaseRequest
            (
                ProductId: request.updateReq.ProductId,
                PhaseId: phaseId,
                Quantity: request.updateReq.quantity,
                AvailableQuantity: request.updateReq.quantity,
                CompanyId: request.updateReq.CompanyId
            ));
            _productPhaseRepository.AddProductPhase(productPhase);
        }
        else
        {
            var updateQuantityPhase3 = productPhase3.Quantity + request.updateReq.quantity;
            var updateAvailableQuantityPhase3 = productPhase3.AvailableQuantity + request.updateReq.quantity;
            productPhase3.UpdateQuantityPhase(updateQuantityPhase3, updateAvailableQuantityPhase3);
            _productPhaseRepository.UpdateProductPhase(productPhase3);
        }

        await _unitOfWork.SaveChangesAsync();

        return Result.Success.Update();

    }
}