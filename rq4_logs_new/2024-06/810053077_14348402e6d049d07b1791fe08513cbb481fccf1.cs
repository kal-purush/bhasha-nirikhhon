using backend_carhelp.shared.Domain.Repositories;
using backend_carhelp.Workshop_management.Domain.Model.Aggregates;
using backend_carhelp.Workshop_management.Domain.Model.Commands;
using backend_carhelp.Workshop_management.Domain.Repositories;
using backend_carhelp.Workshop_management.Domain.Services;

namespace backend_carhelp.Workshop_management.Application.Internal.CommandServices;

public class VehicleCommandService(IVehicleRepository vehicleRepository, IUnitOfWork unitOfWork) : IVehicleCommandService 
{
    public async Task<Vehicle?> Handle(CreateVehicleCommand command)
    {
        var vehicle = new Vehicle(command);
        try
        {
            await vehicleRepository.AddAsync(vehicle);
            await unitOfWork.CompleteAsync();
            return vehicle;
        }
        catch (Exception e)
        {
            Console.WriteLine($"An error occurred while creating the vehicle: {e.Message}");
            return null;
        }
    }
}