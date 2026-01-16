import { style } from '@vanilla-extract/css';
import { global } from '@/styles/global.css';

export const content = style({
  width: '792px',
  '@media': {
    'screen and (min-width: 768px) and (max-width: 1199px)': {
      width: '429px',
    },
    'screen and (min-width: 320px) and (max-width: 767px)': {
      width: '344px',
    },
  },
});

export const contentHeader = style({
  display: 'flex',
  justifyContent: 'space-between',
  alignItems: 'center',
  marginBottom: '24px',
});

export const history = style({
  fontWeight: '700',
  fontSize: '3.2rem',
  lineHeight: '3.8rem',
});

export const dropdown = style({
  width: '120px',
  height: '42px',
  backgroundColor: `${global.color.gray[200]}`,
  borderRadius: '10px',
});

export const list = style({
  display: 'flex',
  flexDirection: 'column',
  gap: '24px',
  width: '100%',
});