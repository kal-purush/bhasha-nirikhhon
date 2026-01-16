import { style } from '@vanilla-extract/css';
import { global } from '@/styles/global.css';

export const background = style({
  position: 'fixed',
  top: '0',
  left: '0',
  height: '100%',
  width: '100%',
  display: 'flex',
  justifyContent: 'center',
  alignItems: 'center',
  zIndex: '10',
});

export const modalArea = style({
  backgroundColor: '#ffffff',
  // height: 'fit-content',
  // width: 'fit-content',
  height: '500px',
  width: '300px',

  padding: '3.2rem 2.4rem',
  borderRadius: '2.4rem',
  border: `1px solid ${global.color.gray[300]}`,
  boxShadow: '0 4px 16px 0 rgb(11 22 11 / 5%)',
});

export const header = style({});

export const miniHeader = style({});