import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faStar as faStarFull, faStarHalfAlt } from '@fortawesome/free-solid-svg-icons';
import { faStar as faStarEmpty } from '@fortawesome/free-regular-svg-icons';

const getStars = (score) => {
  const floorScore = Math.floor(score);
  const reminder = score - floorScore;
  let stars = [];

  for(let i = 1; i <= 5; i++) {
    if (i <= floorScore) {
      stars.push(<FontAwesomeIcon icon={faStarFull} key={i} />);
    } else if (i === floorScore + 1) {
      if (reminder > 0.67) {
        stars.push(<FontAwesomeIcon icon={faStarFull} key={i} />);
      } else if (reminder <= 0.67 && reminder >= 0.34) {
        stars.push(<FontAwesomeIcon icon={faStarHalfAlt} key={i} />);
      } else {
        stars.push(<FontAwesomeIcon icon={faStarEmpty} key={i} />);
      }
    } else {
      stars.push(<FontAwesomeIcon icon={faStarEmpty} key={i} />);
    }
  }
  
  return stars;
}

export default getStars;