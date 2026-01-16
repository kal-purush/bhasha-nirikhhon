using Application.Abstractions.Data;
using Contract.Abstractions.Messages;
using Contract.Abstractions.Shared.Results;
using Contract.Services.MonthlyCompanySalary.Creates;
using Contract.Services.ShipmentDetail.Share;
using Domain.Entities;

namespace Application.UserCases.Commands.MonthlyCompanySalaries;

internal sealed class CreateMonthlyCompanySalaryCommandHandler
    (IShipmentRepository _shipmentRepository,
    IShipmentDetailRepository _shipmentDetailRepository,
    IMonthlyCompanySalaryRepository _monthlyCompanySalaryRepository,
    IProductPhaseSalaryRepository _productPhaseSalaryRepository,
    ICompanyRepository _companyRepository,
    IUnitOfWork _unitOfWork
    ) : ICommandHandler<CreateMonthlyCompanySalaryCommand>
{
    public async Task<Result.Success> Handle(CreateMonthlyCompanySalaryCommand request, CancellationToken cancellationToken)
    {
        var thirdPartyCompanies = await _companyRepository.GetThirdPartyCompany();
        var companyIds = thirdPartyCompanies.Select(x => x.Id).ToList();
        int month = request.Month;
        int year = request.Year;

        var monthlyCompanySalaries = new List<MonthlyCompanySalary>();

        foreach (var companyId in companyIds)
        {
            var receivedShipments = await _shipmentRepository.GetShipmentByCompanyIdAndMonthAndYearAsync(companyId, month, year, true);
            var sendShipments = await _shipmentRepository.GetShipmentByCompanyIdAndMonthAndYearAsync(companyId, month, year, false);
            var productPhaseSalaries = await _productPhaseSalaryRepository.GetAllProductPhaseSalaryAsync();

            decimal materialPrice = 0;
            decimal productSalary = 0;
            decimal productBrokenSalary = 0;
            decimal totalSalary = 0;


            if (receivedShipments != null)
            {
                var shipmentDetails = receivedShipments
                    .SelectMany(x => x.ShipmentDetails).Where(x => x.ProductPhaseType == ProductPhaseType.NO_PROBLEM)
                    .ToList();

                if (shipmentDetails.Any())
                {
                    productSalary = CalculateProductSalary(shipmentDetails, productPhaseSalaries);
                }

            }
            if (sendShipments != null)
            {
                var shipmentDetails = sendShipments
                    .SelectMany(x => x.ShipmentDetails)
                    .Where(x => x.ProductPhaseType == ProductPhaseType.NO_PROBLEM).ToList();

                if (shipmentDetails.Any())
                {
                    materialPrice = CalculateMaterial(shipmentDetails);
                }

                var productBrokenShipmentDetails = sendShipments
                    .SelectMany(x => x.ShipmentDetails)
                    .Where(x => x.ProductPhaseType == ProductPhaseType.THIRD_PARTY_NO_FIX_ERROR).ToList();
                if (productBrokenShipmentDetails.Any())
                {
                    productBrokenSalary = CalculateProductSalary(productBrokenShipmentDetails, productPhaseSalaries);
                }
            }

            totalSalary = productSalary - materialPrice - productBrokenSalary;

            var monthlyCompanySalary = MonthlyCompanySalary
                    .Create(new CreateMonthlyCompanySalaryRequest
                                (
                                    CompanyId: companyId,
                                    Month: month,
                                    Year: year,
                                    Salary: totalSalary
                                ));
            monthlyCompanySalaries.Add(monthlyCompanySalary);
        }

        _monthlyCompanySalaryRepository.AddRange(monthlyCompanySalaries);
        await _unitOfWork.SaveChangesAsync();
        return Result.Success.Create();
    }

    private decimal CalculateProductSalary(List<ShipmentDetail> shipmentDetails, List<ProductPhaseSalary> productPhaseSalaries)
    {
        return shipmentDetails
            .Join<ShipmentDetail, ProductPhaseSalary, (Guid, Guid), (double, decimal)>(
                productPhaseSalaries,
                sd => (sd.ProductId.Value, sd.PhaseId.Value),
                pps => (pps.ProductId, pps.PhaseId),
                (sd, pps) => (sd.Quantity, pps.SalaryPerProduct)
            )
            .Where(x => x.Item1 > 0 && x.Item2 > 0 && Decimal.Multiply((decimal)x.Item1, x.Item2) > 0)
            .Sum(x => Decimal.Multiply((decimal)x.Item1, x.Item2));
    }

    private decimal CalculateMaterial(List<ShipmentDetail> shipmentDetails)
    {
        return shipmentDetails
            .Where(x => x.MaterialId.HasValue)
            .Sum(x => Decimal.Multiply(x.MaterialPrice, (decimal)x.Quantity));
    }
}
