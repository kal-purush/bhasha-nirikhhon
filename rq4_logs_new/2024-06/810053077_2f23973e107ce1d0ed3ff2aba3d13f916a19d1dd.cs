using backend_carhelp.Iam.Domain.Model.Commands;
using backend_carhelp.Iam.Domain.Model.Queries;
using backend_carhelp.Iam.Domain.Repositories;
using backend_carhelp.Iam.Domain.Services;
using backend_carhelp.shared.Domain.Repositories;
using backend_carhelp.Users.Domain.Model.Entities;

namespace backend_carhelp.Iam.Application.Internal.CommandServices;

public class NotificationCommandService : INotificationCommandService
{
    private readonly INotificationRepository _notificationRepository;
    private readonly IUnitOfWork _unitOfWork;
    private readonly IUserRepository _userRepository;
    private INotificationCommandService _notificationCommandServiceImplementation;


    public NotificationCommandService(INotificationRepository notificationRepository, IUnitOfWork unitOfWork, IUserRepository userRepository)
    {
        _notificationRepository = notificationRepository;
        _unitOfWork = unitOfWork;
        _userRepository = userRepository;
    }


    public async Task<Notification?> Handle(CreateNotificationCommand command)
    {
        var user = await _userRepository.GetByIdAsync(command.UserId);
        if (user == null)
        {
            Console.WriteLine($"User with Id {command.UserId} does not exist.");
            return null;
        }

        var notification = new Notification
        {
            UserId = command.UserId
        };
        try
        {
            await _notificationRepository.AddAsync(notification);
            await _unitOfWork.CompleteAsync();
            return notification;
        }
        catch (Exception e)
        {
            Console.WriteLine($"An error occurred while creating the notification: {e.Message}");
            return null;
        }
    }

    public async Task Handle(DeleteNotificationCommand command)
    {
        try
        {
            await _notificationRepository.DeleteNotificationByIdAsync(command.Id);
            await _unitOfWork.CompleteAsync();
        }
        catch (Exception e)
        {
            Console.WriteLine($"An error occurred while deleting the notification: {e.Message}");
        }
    }
}