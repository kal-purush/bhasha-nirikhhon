import { globalStyle } from '@vanilla-extract/css';

globalStyle('html', {
  height: '100%',
});

globalStyle('body', {
  fontFamily: 'Roboto, sans-serif',
  boxSizing: 'border-box',
  fontSize: '1rem',
});

globalStyle('main', {
  marginTop: '60px',
});

globalStyle('#nprogress .bar', {
  height: '4px',
});
globalStyle('#__next', {
  display: 'flex',
  flexDirection: 'column',
  height: '100%',
  paddingBottom: '64px',
});

globalStyle('*,*::after,*::before', {
  boxSizing: 'inherit',
  margin: 0,
  padding: 0,
});

globalStyle('fieldset', {
  border: 'none',
});

globalStyle('fieldset > *:not(:last-child)', {
  marginBottom: '16px',
});

globalStyle('button,input,select', {
  fontFamily: 'inherit',
});