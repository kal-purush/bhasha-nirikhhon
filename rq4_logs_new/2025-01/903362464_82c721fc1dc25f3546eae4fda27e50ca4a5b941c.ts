import { style } from '@vanilla-extract/css';

export const layout = style({
  width: 'min(62.5%, 100%)',
  margin: '7.2rem auto auto',
  display: 'grid',
  gridTemplateColumns: '32% 1fr',
  gridGap: '2.4rem',
  maxWidth: '1200px',

  '@media': {
    'screen and (min-width: 768px) and (max-width: 1199px)': {
      width: 'min(93.54%, 100%)',
      margin: '2.4rem 2.4rem auto',
      gridTemplateColumns: '33.7% 1fr',
    },
    'screen and (max-width: 767px)': {
      width: 'min(89.3%, 100%)',
      gridTemplateColumns: '1fr',
    },
  },
});