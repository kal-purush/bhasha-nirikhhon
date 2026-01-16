using IntelliCook.AppController.Api.Models.Health;

namespace IntelliCook.AppController.Api.Extensions;

public static class AuthContractExtensions
{
    public static HealthStatusModel ToHealthStatusModel(this Auth.Contract.Health.HealthStatusModel model)
    {
        return model switch
        {
            Auth.Contract.Health.HealthStatusModel.Healthy => HealthStatusModel.Healthy,
            Auth.Contract.Health.HealthStatusModel.Degraded => HealthStatusModel.Degraded,
            Auth.Contract.Health.HealthStatusModel.Unhealthy => HealthStatusModel.Unhealthy,
            _ => throw new ArgumentOutOfRangeException(nameof(model), model, null)
        };
    }

    public static HealthCheckModel ToHealthCheckModel(this Auth.Contract.Health.HealthCheckModel model)
    {
        return new HealthCheckModel
        {
            Name = model.Name,
            Status = model.Status.ToHealthStatusModel()
        };
    }

    public static HealthGetResponseModel ToHealthGetResponseModel(
        this Auth.Contract.Health.HealthGetResponseModel model)
    {
        return new HealthGetResponseModel
        {
            Status = model.Status.ToHealthStatusModel(),
            Checks = model.Checks.Select(ToHealthCheckModel)
        };
    }
}