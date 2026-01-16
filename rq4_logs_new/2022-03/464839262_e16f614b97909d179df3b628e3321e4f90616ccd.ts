export type MediasKeys = 'facebook' | 'twitter' | 'instagram' | 'youtube';

export interface IbigCardsMock {
  socialMedia: MediasKeys;
  profile: string;
  actualSubscribers: number;
  update: number;
  status: 'positive' | 'negative';
}

export const bigCardsMock: IbigCardsMock[] = [
  {
    socialMedia: 'facebook',
    profile: '@nathanf',
    actualSubscribers: 1987,
    update: 12,
    status: 'positive',
  },
  {
    socialMedia: 'twitter',
    profile: '@nathanf',
    actualSubscribers: 1044,
    update: 99,
    status: 'positive',
  },
  {
    socialMedia: 'instagram',
    profile: '@realnathanf',
    actualSubscribers: 11000,
    update: 1099,
    status: 'positive',
  },
  {
    socialMedia: 'youtube',
    profile: 'Nathan F.',
    actualSubscribers: 8239,
    update: 144,
    status: 'negative',
  },
];