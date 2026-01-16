import { Link } from 'react-router-dom';
import { URL } from '../utility/Constants';
import { getDateFromISO } from '../utility/DateTimeHelper';

const Homepage = (props) => {
    return (
        <div className="container">
            {props.data.map(review => (
                <div key={review.id} className="card">
                    <div className="card__header">
                        <img src={`${URL}${review.attributes.image.data.attributes.url}`}/>
                    </div>
                    <div className="card__body">
                        <div>
                            {review.attributes.categories.data.map((category, index) => <span key={index} className="tag tag-red">{category.attributes.name}</span>)}
                        </div>
                        <h4>{review.attributes.title}</h4>
                        <p>{review.attributes.body.substring(0, 200)}...</p>
                        <Link to={`/details/${review.id}`}>Read more</Link>
                    </div>
                    <div className="card__footer">
                        <div className="user">
                            <img src={`${URL}${review.attributes.avatar.data.attributes.url}`} className="user__image" width="40" height="40" />
                            <div className="user__info">
                                <h5>{review.attributes.author}</h5>
                                <small>{getDateFromISO(review.attributes.updatedAt)}</small>
                            </div>
                        </div>
                    </div>
                </div>))}
        </div>
    )
}

export default Homepage;