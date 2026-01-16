using Backend.Application.LotData.Dto;
using Backend.Application.LotData.IRepository;

namespace Backend.Application.LotData.UseCases;

/// <summary>
/// Обработчик получения списка лотов по аукциону
/// </summary>
public class GetLotsByAuctionHandler
{
    /// <summary>
    /// Репозиторий лота
    /// </summary>
    private readonly ILotRepository _lotRepository;

    /// <summary>
    /// .ctor
    /// </summary>
    /// <param name="lotRepository">Репозиторий лота</param>
    public GetLotsByAuctionHandler(ILotRepository lotRepository)
    {
        _lotRepository = lotRepository;
    }

    /// <summary>
    /// Получение списка лотов
    /// </summary>
    /// <returns>список лотов</returns>
    public async Task<IReadOnlyCollection<LotDto>> GetLotsByAuction(Guid auctionId)
    {
        var lots = await _lotRepository.SelectManyByAuctionAsync(auctionId);

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