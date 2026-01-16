import Image5 from '../../assets/imgs/6.png';
import Image6 from '../../assets/imgs/7.png';
import Image7 from '../../assets/imgs/8.png';
import Image8 from '../../assets/imgs/10.png';
import { CardContentProps, ImageType } from '../../interfaces';

export const image = [Image5, Image6, Image7, Image8];

export const imageContent: ImageType = {
  src: Image5,
  alt: 'image 5',
};

export const MockData: CardContentProps = {
  data: {
    src: Image5,
    alt: 'image 5',
  },

  contentHard: {
    contentHeader: 'Chocolate Latte',
    contentRateStar: '4',
    contentText:
      'Lorem, ipsum dolor sit amet consectetur adipisicing elit. Soluta.',
    view: 360,
    price: 5,
  },
};

export const MockDataArray: CardContentProps[] = [
  {
    data: {
      src: Image5,
      alt: 'image 5',
    },

    contentHard: {
      contentHeader: 'Chocolate Latte',
      contentRateStar: '4',
      contentText:
        'Lorem, ipsum dolor sit amet consectetur adipisicing elit. Soluta.',
      view: 180,
      price: 5,
    },
  },
  {
    data: {
      src: Image6,
      alt: 'image 6',
    },

    contentHard: {
      contentHeader: 'Mocha Latte',
      contentRateStar: '5.0',
      contentText:
        'Lorem, ipsum dolor sit amet consectetur adipisicing elit. Soluta.',
      view: 450,
      price: 2.7,
    },
  },
  {
    data: {
      src: Image7,
      alt: 'image 7',
    },

    contentHard: {
      contentHeader: 'Chocolate Latte',
      contentRateStar: '4',
      contentText:
        'Lorem, ipsum dolor sit amet consectetur adipisicing elit. Soluta.',
      view: 390,
      price: 2.9,
    },
  },
  {
    data: {
      src: Image8,
      alt: 'image 8',
    },

    contentHard: {
      contentHeader: 'Chocolate Latte',
      contentRateStar: '4',
      contentText:
        'Lorem, ipsum dolor sit amet consectetur adipisicing elit. Soluta.',
      view: 360,
      price: 2.8,
    },
  },
];

// export interface constHardImage {
//   contentHeader: string;
//   contentRateStar: string;
//   contentText: string;
//   price: number;
// }