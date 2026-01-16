import { fontSizes, fontWeights } from '@/styles/fontStyles.css';
import { style } from '@vanilla-extract/css';
import { global } from '@/styles/global.css';

export const activitiesPageContainer = style({
  marginBottom: '12rem',
  maxWidth: '80rem',
  margin: '0 auto',
});

export const topLayout = style({
  display: 'flex',
  justifyContent: 'space-between',
  maxWidth: '80rem',
  margin: '0 auto',
  marginTop: '7.2rem',
  marginBottom: '3.4rem',
});

export const h2Title = style([fontSizes.text3xl, fontWeights.bold]);

export const createButton = style([
  {
    backgroundColor: global.color.nomadBlack,
    color: '#fff',
    padding: '0.8rem 1.6rem',
    borderRadius: '0.4rem',
  },
  fontSizes.textLg,
]);