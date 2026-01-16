namespace Backend.Application.AuctionData.Dto;

/// <summary>
/// Класс для создания аукциона
/// </summary>
public class CreateAuctionDto
{
    /// <summary>
    /// Название аукциона
    /// </summary>
    public string? Name { get; set; }

    /// <summary>
    /// Описание аукциона
    /// </summary>
    public string? Description { get; set; }

    /// <summary>
    /// Уникальный идентификатор пользователя-создателя
    /// </summary>
    public Guid AuthorId { get; set; }
}