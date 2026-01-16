import React from 'react';
import Chip from '@material-ui/core/Chip';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import {
  faTint, faStar, faBolt, faFire, faHandRock,
  faFeatherAlt, faLeaf, faSkull, faGlobeEurope,
  faBrain, faDiceD20, faSnowflake, faBug, faDragon,
  faGhost, faAdjust, faDrumSteelpan, faHatWizard,
} from '@fortawesome/free-solid-svg-icons';

const Chips = ({
  pokemonType, label,
}) => {
  console.log('ty', pokemonType);
  console.log('lab', label);

  const chipColor = (type) => {
    if (type === 'normal') {
      return {
        color: '#a3a375',
        icon: <FontAwesomeIcon icon={faStar} />,
      };
    } if (type === 'water') {
      return {
        color: '#668cff',
        icon: <FontAwesomeIcon icon={faTint} />,
      };
    } if (type === 'electric') {
      return {
        color: '#ffdb4d',
        icon: <FontAwesomeIcon icon={faBolt} />,
      };
    }
    if (type === 'fire') {
      return {
        color: '#ff9933',
        icon: <FontAwesomeIcon icon={faFire} />,
      };
    }
    if (type === 'fighting') {
      return {
        color: '#ff1a1a',
        icon: <FontAwesomeIcon icon={faHandRock} />,
      };
    }
    if (type === 'flying') {
      return {
        color: '#d9b3ff',
        icon: <FontAwesomeIcon icon={faFeatherAlt} />,
      };
    }
    if (type === 'grass') {
      return {
        color: '#33ff33',
        icon: <FontAwesomeIcon icon={faLeaf} />,
      };
    }
    if (type === 'poison') {
      return {
        color: '#9900cc',
        icon: <FontAwesomeIcon icon={faSkull} />,
      };
    }
    if (type === 'ground') {
      return {
        color: '#996600',
        icon: <FontAwesomeIcon icon={faGlobeEurope} />,
      };
    }
    if (type === 'psychic') {
      return {
        color: '#ff0066',
        icon: <FontAwesomeIcon icon={faBrain} />,
      };
    } if (type === 'rock') {
      return {
        color: '#558000',
        icon: <FontAwesomeIcon icon={faDiceD20} />,
      };
    } if (type === 'ice') {
      return {
        color: '#99ddff',
        icon: <FontAwesomeIcon icon={faSnowflake} />,
      };
    }
    if (type === 'bug') {
      return {
        color: '#99e600',
        icon: <FontAwesomeIcon icon={faBug} />,
      };
    }
    if (type === 'dragon') {
      return {
        color: '#6600cc',
        icon: <FontAwesomeIcon icon={faDragon} />,
      };
    } if (type === 'ghost') {
      return {
        color: ' #800040',
        icon: <FontAwesomeIcon icon={faGhost} />,
      };
    } if (type === 'dark') {
      return {
        color: '#663300',
        icon: <FontAwesomeIcon icon={faAdjust} />,
      };
    } if (type === 'steel') {
      return {
        color: '#e6ccff',
        icon: <FontAwesomeIcon icon={faDrumSteelpan} />,
      };
    } if (type === 'fairy') {
      return {
        color: '#ffcce6',
        icon: <FontAwesomeIcon icon={faHatWizard} />,
      };
    }
    return '';
  };
  const { color, icon } = chipColor(pokemonType);

  return (
    <Chip
      variant="outlined"
      size="medium"
      label={label}
      style={{ backgroundColor: color, margin: 4 }}
      icon={icon}
    />
  );
};

export default Chips;