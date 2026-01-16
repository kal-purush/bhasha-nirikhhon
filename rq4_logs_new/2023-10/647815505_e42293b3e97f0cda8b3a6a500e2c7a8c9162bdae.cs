using Backend.Application.LotData.Dto;
using Backend.Application.LotData.IRepository;

namespace Backend.Application.LotData.UseCases;

/// <summary>
/// Обработчик получения списка лотов
/// </summary>
public class GetLotsHandler
{
    /// <summary>
    /// Репозиторий лота
    /// </summary>
    private readonly ILotRepository _lotRepository;

    /// <summary>
    /// .ctor
    /// </summary>
    /// <param name="lotRepository">Репозиторий лота</param>
    public GetLotsHandler(ILotRepository lotRepository)
    {
        _lotRepository = lotRepository;
    }

    /// <summary>
    /// Получение списка аукционов
    /// </summary>
    /// <returns>список аукционов</returns>
    public async Task<IReadOnlyCollection<LotDto>> GetLots()
    {
        var lots = await _lotRepository.SelectManyAsync();

        return (from lot in lots
            let bets = lot.Bets
            let betsDto = bets.Select(bet => new BetDto
                {
                    Id = bet.Id,
                    Value = bet.Value,
                    LotId = bet.LotId,
                    UserId = bet.UserId,
                    DateTime = bet.DateTime
                })
                .AsParallel()
                .ToList()
            let images = lot.Images
            let imagesDto = images.Select(image => new ImageDto
                {
                    Id = image.Id,
                    LotId = image.LotId,
                    Path = image.Path
                })
                .AsParallel()
                .ToList()
            select new LotDto
            {
                Id = lot.Id,
                Name = lot.Name,
                Description = lot.Description,
                StartPrice = lot.StartPrice,
                BuyoutPrice = lot.BuyoutPrice,
                BetStep = lot.BetStep,
                State = lot.State,
                Bets = betsDto,
                Images = imagesDto
            }).AsParallel().ToList();
    }
}