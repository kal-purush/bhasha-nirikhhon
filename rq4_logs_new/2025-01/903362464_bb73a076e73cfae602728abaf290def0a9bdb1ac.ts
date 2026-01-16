import { style } from '@vanilla-extract/css';

export const overlay = style({
  position: 'fixed',
  top: '0',
  left: '0',
  width: '100',
  height: '100',
  background: 'rgba(0, 0, 0, 0.5)',
  display: 'flex',
  justifyContent: 'center',
  alignItems: 'center',
  zIndex: '1000',
});

export const modal = style({
  width: '540px',
  height: '250px',
  background: '#fff',
  borderRadius: '8px',
  display: 'flex',
  flexDirection: 'column',
  justifyContent: 'center',
  alignItems: 'center',
  boxShadow: '0 4px 6px rgba(0, 0, 0, 0.1)',
});

export const message = style({
  fontSize: '16px',
  color: '#000',
  marginBottom: '20px',
  textAlign: 'center',
});

export const closeButton = style({
  width: '100px',
  height: '40px',
  background: '#000',
  color: '#fff',
  border: 'none',
  borderRadius: '4px',
  cursor: 'pointer',
  fontSize: '14px',
  selectors: {
    '&:hover': {
      background: '#333',
    },
  },
});