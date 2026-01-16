import { style } from '@vanilla-extract/css';
import { global } from '@/styles/global.css';

export const skeleton = style({
  width: '100%',
  height: '116px',
  backgroundColor: `${global.color.gray[100]}`,
  borderRadius: '4px',
  display: 'flex',
  flexDirection: 'column',
  justifyContent: 'space-between',
  padding: '14px 16px',
});

export const text = style({
  display: 'flex',
  flexDirection: 'column',
  gap: '10px',
});

export const line = style({
  height: '16px',
  width: '80px',
  backgroundColor: `${global.color.gray[300]}`,
});