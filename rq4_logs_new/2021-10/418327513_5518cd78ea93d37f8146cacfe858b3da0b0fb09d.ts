import {Card} from "../domain/Card";
import Mapper from "../../../core/infra/Mapper";
import {UniqueEntityID} from "../../../core/domain/UniqueEntityID";

export class CardMap extends Mapper<Card>{
    public static toPersistence(card: Card): any{
        return {
            card_id: card.id.toString(),
            card_index: card.cardIndex,
            card_name: card.cardName
        }
    }

    public static toDomain(raw: any): Card{
        const cardOrError = Card.create({
            cardIndex: raw.card_index,
            cardName: raw.cardName
        }, new UniqueEntityID(raw.card_id));

        cardOrError.isFailure ? console.log(cardOrError.error) : '';

        return cardOrError.isSuccess? cardOrError.getValue() : null;
    }
}