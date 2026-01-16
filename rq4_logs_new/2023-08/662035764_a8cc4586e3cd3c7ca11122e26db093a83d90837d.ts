import { Review } from '../types/review';

export const reviews: Review[] = [
  {
    id: 'b67ddfd5-b953-4a30-8c8d-bd083cd6b62a',
    date: '2022-05-08T14:13:56.569Z',
    user: {
      name: 'Oliver Conner',
      avatarUrl: 'https://13.design.pages.academy/static/avatar/2.jpg',
      isPro: false
    },
    comment: 'A quiet cozy and picturesque that hides behind a a river by the unique lightness of Amsterdam.',
    rating: 4
  },
  {
    id: '31dc3b43-dc3b-4401-a9fe-d3581722c626',
    comment: 'We loved it so much, the house, the veiw, the location just great.. Highly reccomend :)',
    date: '2023-07-09T21:00:00.455Z',
    rating: 5,
    user: {
      name: 'Max',
      avatarUrl: 'https://13.design.pages.academy/static/avatar/10.jpg',
      isPro: true
    }
  },
  {
    id: '079d6414-b05c-427e-b5d8-7049694d43a1',
    comment: 'What an amazing view! The house is stunning and in an amazing location. The large glass wall had an amazing view of the river!',
    date: '2023-07-08T21:00:00.455Z',
    rating: 5,
    user: {
      name: 'Emely',
      avatarUrl: 'https://13.design.pages.academy/static/avatar/5.jpg',
      isPro: true
    }
  }
];