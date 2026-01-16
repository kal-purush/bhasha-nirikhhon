import { style } from '@vanilla-extract/css';
import { global } from '@/styles/global.css';

export const button = style({
  border: 'none',
  fontWeight: '700',
  fontSize: '16px',
});

export const solid = style({
  backgroundColor: `${global.color.nomadBlack}`,
  color: 'white',
  fontSize: '16px',
});
export const outline = style({
  backgroundColor: 'transparent',
  border: `1px solid ${global.color.nomadBlack}`,
  color: `${global.color.nomadBlack}`,
  fontSize: '16px',
});

export const disabled = style({
  backgroundColor: `${global.color.gray[700]}`,
  color: `#FFFFFF`,
  fontSize: '16px',
});