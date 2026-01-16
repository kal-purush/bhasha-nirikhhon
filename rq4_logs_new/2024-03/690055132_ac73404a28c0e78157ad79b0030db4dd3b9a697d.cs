using AirQuality.Core.DAL.Models;
using AirQuality.SensorService.DTO;
using AirQuality.SensorService.Tests.Helpers;

namespace AirQuality.SensorService.Tests.Fake;

public static partial class FakeData
{
    public static CreateStationDataDto GetCreateStationDataDto()
    {
        var temperature = TestHelper.GetRandomFloat(StationData.TemperatureMinValue, StationData.TemperatureMaxValue);
        var humidity = TestHelper.GetRandomInt(StationData.HumidityMinValue, StationData.HumidityMaxValue);
        var pm1 = TestHelper.GetRandomInt(StationData.Pm1MinValue, StationData.Pm1MaxValue);
        var pm2_5 = TestHelper.GetRandomInt(StationData.Pm2_5MinValue, StationData.Pm2_5MaxValue);
        var pm10 = TestHelper.GetRandomInt(StationData.Pm10MinValue, StationData.Pm10MaxValue);
        var co = TestHelper.GetRandomInt(StationData.CoMinValue, StationData.CoMaxValue);
        var pressure = TestHelper.GetRandomInt(StationData.PressureMinValue, StationData.PressureMaxValue);
        
        var createStationDataDto = new CreateStationDataDto(
            temperature,
            humidity,
            pm1,
            pm2_5,
            pm10,
            co,
            pressure
        );

        return createStationDataDto;
    }
}