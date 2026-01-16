import { ICard } from '../../@shared/rest-shared/card';

// tslint:disable: max-line-length
const jellyfishCard: ICard = {
  id: `jellyfish`,
  text: {
    en: ``,
    fr: `Lorsque cette carte est détruite, placez une case Eau à la place`,
  },
  lore: {
    en: ``,
    fr: ``,
  },
  imageUrl: `https://static.thefirstspine.fr/placeholder.png`,
  name: {
    en: ``,
    fr: `Méduse`,
  },
  stats: {
    life: 3,
    bottom: {
      defense: 0,
      strength: 6,
    },
    left: {
      defense: 0,
      strength: 2,
      capacity: 'aura',
    },
    right: {
      defense: 0,
      strength: 2,
      capacity: 'aura',
    },
    top: {
      defense: 1,
      strength: 1,
    },
  },
  type: 'creature',
};

export default jellyfishCard;