import { createTheme, style } from '@vanilla-extract/css';

const size = 30;

export const [themeClass, vars] = createTheme({
  duration: '0s',
});

export const root = style([
  themeClass,
  {
    position: 'relative',
    width: '300px',
    height: `${size}px`,
    margin: '24px 0 !important',

    selectors: {
      '&:before': {
        content: '',
        position: 'absolute',
        top: `${size / 2 - 1}px`,
        left: `${size / 2}px`,
        right: `${size / 2}px`,
        background: 'rgba(0, 0, 0, 0.1)',
        height: '2px',
      },

      '&:after': {
        content: '',
        position: 'absolute',
        left: '0',
        top: '0',
        width: `${size}px`,
        height: `${size}px`,
        background: '#127ff9',
        borderRadius: `${size / 2}px`,
      },

      '&:hover:after': {
        left: `calc(100% - ${size}px)`,
        transition: `${vars.duration} linear`,
      },
    },
  },
]);