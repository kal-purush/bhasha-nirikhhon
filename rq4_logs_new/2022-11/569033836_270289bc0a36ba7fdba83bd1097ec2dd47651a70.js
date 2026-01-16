import { LazyLoadImage } from "react-lazy-load-image-component";
import style from './style.module.css'
const Card = ({ card }) => {
    console.log(card);
    return (
        <div>
            <h2 class={style.card_title}>{card.name}</h2>
            <LazyLoadImage
            className={style.card}
                src={card.imageUrl}
                alt={card.name}
                width="200"
                referrerPolicy='no-referrer'
            />
        </div>
    )
}

export default Card