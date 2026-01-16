import { style } from '@vanilla-extract/css';

export const container = style({
  display: 'flex',
  flexDirection: 'column',
});

export const item = style({
  padding: '0 1rem',
  border: '1px solid #ccc',
});

export const trigger = style({
  display: 'flex',
  alignContent: 'space-between',
  width: '100%',
  padding: '1.2rem 0',
});

export const title = style({
  flex: 1,
  textAlign: 'left',
});

export const content = style({
  display: 'none',
  padding: '1rem 0',
  borderTop: '1px dashed #ccc',
  fontSize: '0.875rem',
});

export const show = style({
  display: 'block',
});