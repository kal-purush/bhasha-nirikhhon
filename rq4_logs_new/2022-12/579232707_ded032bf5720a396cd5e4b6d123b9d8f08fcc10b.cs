using Catcheap.API.Interfaces.IRepository;
using Catcheap.API.Interfaces.IService;
using Catcheap.API.Models.CarModels;

namespace Catcheap.API.Services;

public class CarChargeService : ICarChargeService
{
    private readonly ICarChargeRepository _chargeRepository;

    public CarChargeService(ICarChargeRepository chargeRepository)
    {
        _chargeRepository = chargeRepository;
    }

    public TimeSpan CalculateDurationOfCharge(CarCharge carCharge)
    {
        return carCharge.EndOfCharge.Subtract(carCharge.StartOfCharge);
    }

    public double CalculateChargedKWh(CarCharge carCharge)
    {
        TimeSpan durationOfCharge = CalculateDurationOfCharge(carCharge);

        return (carCharge.ChargingPower * (durationOfCharge.TotalHours / 1000));
    }
}