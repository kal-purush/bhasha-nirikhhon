using Application.Abstractions.Data;
using Application.Abstractions.Services;
using Contract.Abstractions.Messages;
using Contract.Abstractions.Shared.Results;
using Contract.Services.MonthlyCompanySalary.Queries;
using Contract.Services.MonthlyCompanySalary.ShareDtos;
using Contract.Services.ShipmentDetail.Share;
using Domain.Entities;
using System.Linq;

namespace Application.UserCases.Queries.MonthlyCompanySalaries;

internal sealed class GetMonthlyCompanySalaryByIdQueryHandler
    (IMonthlyCompanySalaryRepository _monthlyCompanySalaryRepository,
    IShipmentRepository _shipmentRepository,
    IShipmentDetailRepository _shipmentDetailRepository,
    IProductPhaseSalaryRepository _productPhaseSalaryRepository,
    ICloudStorage _cloudStorage
    ) : IQueryHandler<GetMonthlyCompanySalaryByIdQuery, MonthlyCompanySalaryDetailResponse>
{
    public async Task<Result.Success<MonthlyCompanySalaryDetailResponse>> Handle(GetMonthlyCompanySalaryByIdQuery request, CancellationToken cancellationToken)
    {
        var monthlyCompanySalary = await _monthlyCompanySalaryRepository.GetMonthlyCompanySalaryByIdAsync(request.MonthlyCompanySalaryId);
        var receivedShipments = await _shipmentRepository.GetShipmentByCompanyIdAndMonthAndYearAsync(request.CompanyId, request.Month, request.Year, true);
        var sendShipments = await _shipmentRepository.GetShipmentByCompanyIdAndMonthAndYearAsync(request.CompanyId, request.Month, request.Year, false);
        var productPhaseSalaries = await _productPhaseSalaryRepository.GetAllProductPhaseSalaryAsync();

        var materialResponses = sendShipments
               .SelectMany(sh => sh.ShipmentDetails)
               .Where(sd => sd.MaterialId.HasValue)
               .Select(sd => new MaterialExportReponse(
                   MaterialId: sd.MaterialId.Value,
                   MaterialName: sd.Material.Name,
                   MaterialUnit: sd.Material.Unit,
                   Quantity: sd.Quantity,
                   Price: sd.MaterialPrice
               ))
               .ToList();

        var productExportResponses = await GetProductExportResponses(receivedShipments, productPhaseSalaries, ProductPhaseType.NO_PROBLEM);
        var productBrokenResponses = await GetProductExportResponses(sendShipments, productPhaseSalaries, ProductPhaseType.THIRD_PARTY_NO_FIX_ERROR);

        var totalSalaryProduct = productExportResponses.Sum(pe => Decimal.Multiply((decimal)pe.Quantity, pe.Price));
        var totalSalaryMaterial = materialResponses.Sum(me => Decimal.Multiply((decimal)me.Quantity, me.Price));
        var totalSalaryBroken = productBrokenResponses.Sum(pb => Decimal.Multiply((decimal)pb.Quantity, pb.Price));
        var totalSalaryTotal = totalSalaryProduct - totalSalaryMaterial - totalSalaryBroken;
        var response = new MonthlyCompanySalaryDetailResponse(
                CompanyId: request.CompanyId,
                Month: request.Month,
                Year: request.Year,
                Salary: monthlyCompanySalary.Salary, // Assign 0 or appropriate logic if required
                Status: monthlyCompanySalary.Status, // Assuming status is pending, update as necessary
                Note: monthlyCompanySalary.Note,
                TotalSalaryProduct: totalSalaryProduct,
                TotalSalaryMaterial: totalSalaryMaterial,
                TotalSalaryBroken: totalSalaryBroken,
                TotalSalaryTotal: totalSalaryTotal,
                MaterialResponses: materialResponses,
                ProductExportResponses: productExportResponses,
                ProductBrokenResponses: productBrokenResponses
            );

        return Result.Success<MonthlyCompanySalaryDetailResponse>.Get(response);

    }

    private async Task<List<ProductExportResponse>> GetProductExportResponses(List<Shipment> shipments, List<ProductPhaseSalary> productPhaseSalaries, ProductPhaseType phaseType)
    {
        var shipmentDetails = shipments
            .SelectMany(sh => sh.ShipmentDetails)
            .Where(sd => sd.ProductPhaseType == phaseType)
            .Join(productPhaseSalaries,
                sd => (sd.ProductId.Value, sd.PhaseId.Value),
                pps => (pps.ProductId, pps.PhaseId),
                (sd, pps) => new
                {
                    ShipmentDetail = sd,
                    ProductPhaseSalary = pps
                })
            .ToList();

        var productExportResponses = new List<ProductExportResponse>();

        foreach (var item in shipmentDetails)
        {
            var sd = item.ShipmentDetail;
            var pps = item.ProductPhaseSalary;

            var productImage = await _cloudStorage
                .GetSignedUrlAsync(sd.Product.Images
                                    .FirstOrDefault(img => img.IsMainImage)
                                    ?.ImageUrl ?? "ImageNotFound");

            productExportResponses.Add(new ProductExportResponse(
                ProductId: sd.ProductId.Value,
                ProductCode: sd.Product.Code,
                ProductName: sd.Product.Name,
                ProductImage: productImage,
                PhaseId: sd.PhaseId.Value,
                PhaseName: sd.Phase.Name,
                Quantity: sd.Quantity,
                Price: pps.SalaryPerProduct
            ));
        }

        return productExportResponses;
    }

}