using NetService.Application.DTOs.Notifications;

namespace NetService.Domain.Interfaces;

public interface INotificationRepository
{
    Task<List<NotificationDto>> GetAllNotificationsAsync();
    Task<NotificationDto?> GetNotificationByIdAsync(int id);
    Task<NotificationDto> CreateNotificationAsync(NewNotificationDto newNotificationDto);
    Task<bool> UpdateNotificationAsync(int id, NewNotificationDto updatedNotification);
    Task<bool> DeleteNotificationAsync(int id);
    Task<bool> AddNotificationToUserAsync(AddRemoveNotificationDto dto);
    Task<bool> RemoveNotificationFromUserAsync(AddRemoveNotificationDto dto);
}