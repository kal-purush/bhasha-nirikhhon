import { Product } from 'components/ProductCard/types';
import airpods from 'media/images/airpods.jpg';
import { SyntheticEvent } from 'react';

export const ALL_PRODUCTS: Product[] = [
  {
    id: 0,
    name: 'Harman Kardon Speaker',
    rating: 4,
    onRatingChange: (event: SyntheticEvent<Element, Event>, newValue: number | null) => {
      console.log(event, newValue);
    },
    type: 'Electronics',
    curPrice: 1725,
    prevPrice: 1725,
    onSale: false,
    isFavorite: false,
    onFavoriteChange: () => {},
    image: airpods,
  },
];