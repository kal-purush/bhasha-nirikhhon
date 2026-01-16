using backend_carhelp.shared.Domain.Repositories;
using backend_carhelp.Workshop_management.Domain.Model.Aggregates;
using backend_carhelp.Workshop_management.Domain.Model.Commands;
using backend_carhelp.Workshop_management.Domain.Repositories;
using backend_carhelp.Workshop_management.Domain.Services;

namespace backend_carhelp.Workshop_management.Application.Internal.CommandServices;

public class AdvertasingCommandService(IAdvertasingRepository advertasingRepository, IUnitOfWork unitOfWork): IAdvertasingCommandService
{
    public async Task<Advertasing?> Handle(CreateAdvertasingCommand command)
    {
        var advertasing = new Advertasing(command);
        try
        {
            await advertasingRepository.AddAsync(advertasing);
            await unitOfWork.CompleteAsync();
            return advertasing;
        }
        catch (Exception e)
        {
            Console.WriteLine($"An error occurred while creating the advertasing: {e.Message}");
            return null;
        }
    }
}