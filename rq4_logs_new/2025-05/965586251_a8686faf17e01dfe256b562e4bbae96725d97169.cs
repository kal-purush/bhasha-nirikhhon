using DeliveryApp.Core.Domain.Models.CourierAggregate;

namespace DeliveryApp.Core.Ports;

public interface ICourierRepository
{
    /// <summary>
    /// Получение курьера по идентификатору.
    /// </summary>
    /// <param name="id">Идентификатор.</param>
    /// <param name="ct">Токен отмены.</param>
    /// <returns>Курьер.</returns>
    Task<Courier> GetByIdAsync(Guid id, CancellationToken ct);

    /// <summary>
    /// Получение всех свободных курьеров.
    /// </summary>
    /// <param name="ct">Токен отмены.</param>
    /// <returns>Коллекция курьеров.</returns>
    Task<ICollection<Courier>> GetAllFreeAsync(CancellationToken ct);
    
    /// <summary>
    /// Создание курьера.
    /// </summary>
    /// <param name="order">Курьер.</param>
    /// <param name="ct">Токен отмены.</param>
    /// <returns>Результат выполнения.</returns>
    Task CreateAsync(Courier order, CancellationToken ct);

    /// <summary>
    /// Обновление курьера.
    /// </summary>
    /// <param name="order">Курьер.</param>
    void Update(Courier order);
}