using MarGate.Core.CQRS.Command;

namespace MarGate.Basket.Application.Handlers.BasketItem.Commands.CreateBasketItem;

public class CreateBasketItemCommandHandler(IBasketItemWriteRepository basketItemWriteRepository) : CommandHandler<CreateBasketItemCommandRequest, CreateBasketItemCommandResponse>
{

    private readonly IBasketItemWriteRepository _basketItemWriteRepository = basketItemWriteRepository;

    public override Task<CreateBasketItemCommandResponse> Handle(CreateBasketItemCommandRequest request,
        CancellationToken cancellationToken)
    {
        // override edilecek mi...
        var basketItem = new BasketItem()
        {
            ProductId = request.ProductId,
            BasketId = request.BasketId
        };

        var isSuccess = await _basketItemWriteRepository.CreateAsync(basketItem);
        await _basketItemWriteRepository.SaveAsync();

        return new CreateBasketItemCommandResponse()
        {
            IsSuccess = isSuccess
        };
    }
}