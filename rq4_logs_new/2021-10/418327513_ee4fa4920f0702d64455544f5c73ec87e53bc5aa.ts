import {inject, injectable} from "inversify";
import {
    GetOrdersResponseDTO,
    GetOrdersUseCaseRequestDTO,
    GetOrderUseCaseResponse,
    IGetOrdersUseCase
} from "./interfaces/IGetOrdersUseCase";
import {TYPES} from "../../../../infra/inversify/config/types";
import {FetchTraderResponseDTO, IFetchTraderUseCase} from "../shared/interfaces/IFetchTraderUseCase";
import {ICardOrderRepo} from "../../repos/interfaces/ICardOrderRepo";
import {TraderId} from "../../../trader/domain/TraderId";
import {UniqueEntityID} from "../../../../core/domain/UniqueEntityID";
import {CardOrder} from "../../domain/CardOrder";
import {left, right} from "../../../../core/logic/Either";
import {GenericAppError} from "../../../../core/logic/AppError";
import {Result} from "../../../../core/logic/Result";

@injectable()
export class GetOrdersUseCase implements IGetOrdersUseCase {
    private static readonly GET_ORDERS_COUNT: number = 50;
    @inject(TYPES.FetchTraderUseCase) private fetchTraderUseCase: IFetchTraderUseCase;
    @inject(TYPES.CardOrderRepo) private cardOrderRepo: ICardOrderRepo;

    async execute(request?: GetOrdersUseCaseRequestDTO): Promise<GetOrderUseCaseResponse> {
        // fetch trader id
        const fetchTraderResult = await this.fetchTraderUseCase.execute({
            userId: request.userId
        });
        if (fetchTraderResult.isLeft()) {
            return fetchTraderResult;
        }
        const fetchTraderResponseDTO: FetchTraderResponseDTO = fetchTraderResult.value.getValue()
        // fetch card orders by trader id
        const cardOrderTraderId = TraderId.create(new UniqueEntityID(fetchTraderResponseDTO.traderId));
        try {
            const cardOrders: CardOrder[] = await this.cardOrderRepo.findLatestOrdersByTraderId(GetOrdersUseCase.GET_ORDERS_COUNT, cardOrderTraderId);
            // mapping from CardOrder[] to cardOrderInfo[]
            return right(Result.ok<GetOrdersResponseDTO>({
                cardOrders: cardOrders.map(cardOrder => {
                    return {
                        id: cardOrder.id.toString(),
                        price: cardOrder.price.value,
                        cardIndex: cardOrder.cardIndex,
                        status: cardOrder.status.value,
                        type: cardOrder.type.value,
                        orderedTime: cardOrder.orderedTime.getTime()
                    }
                })
            })) as GetOrderUseCaseResponse
        } catch (err) {
            // fetch card orders failed !!
            console.log(err);
            return left(new GenericAppError.UnexpectedError(err)) as GetOrderUseCaseResponse
        }
    }

}