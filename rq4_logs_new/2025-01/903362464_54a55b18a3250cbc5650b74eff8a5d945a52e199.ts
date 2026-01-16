import { globalStyle, style } from '@vanilla-extract/css';
import { global } from '@/styles/global.css';

export const menuContainer = style({
  borderRadius: '0.6rem 0',
});

export const button = style({
  ':hover': {
    backgroundColor: global.color.gray[100],
  },
  ':active': {
    backgroundColor: global.color.gray[100],
  },
});

export const menuItem = style({
  color: global.color.gray[900],
  padding: '1.8rem 4.6rem',
  fontSize: global.fontSize.regular,
  lineHeight: global.lineHeights.small,
  fontWeight: 500,
});

globalStyle('.css-1tktgsa-MuiPaper-root-MuiPopover-paper-MuiMenu-paper', {
  boxShadow: 'none',
  borderRadius: '0.6rem 0',
  border: `1px solid ${global.color.gray[300]}`,
});