using MarGate.Core.CQRS.Command;

namespace MarGate.Basket.Application.Handlers.Basket.Commands.CreateBasket;


public class CreateBasketCommandHandler(IBasketWriteRepository basketWriteRepository) : CommandHandler<CreateBasketCommandRequest, CreateBasketCommandResponse>
{
    private readonly IBasketWriteRepository _basketWriteRepository = basketWriteRepository;

    public override async Task<CreateBasketCommandResponse> Handle(CreateBasketCommandRequest request, CancellationToken cancellationToken)
    {
        //if (request.UserId <= 0)
        //    throw new ArgumentException($"UserId must be greater than zero. Attempted to set: {request.UserId}.");


        //var basket = new Basket(request.UserId);

        //if (request.Items != null && request.Items.Any())
        //{
        //    foreach (var item in request.Items)
        //    {
        //        var basketItem = new BasketItem(item.CatalogId, item.Quantity, item.UnitPrice);
        //        basket.AddItem(basketItem);
        //    }
        //}


        //var isSuccess = await _basketWriteRepository.CreateAsync(basket);
        //await _basketWriteRepository.SaveAsync();

        //return new CreateBasketCommandResponse
        //{
        //    IsSuccess = isSuccess,
        //    BasketId = basket.Id
        //};
    }
}
