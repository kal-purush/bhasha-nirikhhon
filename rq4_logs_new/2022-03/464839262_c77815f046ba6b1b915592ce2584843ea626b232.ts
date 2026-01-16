import { SocialMediaOptions } from '~/domain/SocialMedia.types';

export interface SocialMediaCardCommon {
  formattedMainNumber: string;
  subTitle: string;
  statusNumber: number;
  status: StatusOptions;
}

export type StatusOptions = 'positive' | 'negative';
export type CardsSizeOptions = 'big' | 'small';

export interface TypeInterface {
  type: SocialMediaOptions;
}

// used in components

export interface SocialMediaCardAdapterDTO extends SocialMediaCardCommon {
  size: CardsSizeOptions;
  socialMedia: SocialMediaOptions;
}

export interface SocialMediaCardSmallDTO extends SocialMediaCardCommon, TypeInterface {}

export interface SocialMediaCardBigDTO extends SocialMediaCardCommon, TypeInterface {
  description: string;
  borderColor: string;
}