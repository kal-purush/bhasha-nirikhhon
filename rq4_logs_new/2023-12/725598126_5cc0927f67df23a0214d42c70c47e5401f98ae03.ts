import { Components, Theme } from '@mui/material/styles';

declare module '@mui/material/Button' {
  interface ButtonPropsVariantOverrides {
    primary: true;
    secondary: true;
    navigation: true;
  }
}
export const MuiButton: Components<Theme>['MuiButton'] = {
  defaultProps: {
    variant: 'primary',
    disableRipple: true,
    disableTouchRipple: true,
    disableFocusRipple: true,
  },
  styleOverrides: {
    root: ({ theme }) => ({
      textTransform: 'none',
      color: theme.palette.common.black,
      height: 54,
      borderRadius: 4,
      padding: '10px 32px',
      minWidth: 204,
      letterSpacing: '0.8px',
    }),
    startIcon: {
      '& > *:first-of-type': {
        fontSize: '1.5rem',
      },
    },
    endIcon: {
      marginRight: 0,
      marginLeft: 8,
      '& > *:first-of-type': {
        fontSize: '1.5rem',
      },
    },
  },
  variants: [
    {
      props: { variant: 'primary' },
      style: ({ theme }) => ({
        backgroundColor: '#D9A145',
        color: '#17161D',

        '&:hover': {
          backgroundColor: theme.palette.primary.dark,
        },

        '&:focus': {
          backgroundColor: theme.palette.primary.dark,
        },

        '&:active': {
          boxShadow: '0px 4px 8px 0px rgba(21, 21, 20, 0.15) inset',
          backgroundColor: theme.palette.primary.dark,
        },

        '&:disabled': {
          color: theme.palette.common.black,
          backgroundColor: theme.palette.action.disabledBackground,
        },
      }),
    },
    {
      props: { variant: 'secondary' },
      style: ({ theme }) => ({
        backgroundColor: 'transparent',
        border: '2px solid',
        borderColor: '#D9A145',
        color: '#D9A145',

        '&:hover': {
          color: theme.palette.primary.dark,
          borderColor: theme.palette.primary.dark,
        },
        // focus-visible - для того, щоб фокус був тільки при навігації клавішами
        '&:focus-visible': {
          color: theme.palette.primary.dark,
          borderColor: theme.palette.primary.dark,
          backgroundColor: theme.palette.secondary.dark,
        },

        '&:active': {
          boxShadow: '0px 4px 8px 0px rgba(21, 21, 20, 0.15) inset',

          color: theme.palette.primary.dark,
          borderColor: theme.palette.primary.dark,
        },

        '&:disabled': {
          color: theme.palette.text.disabled,
          borderColor: theme.palette.text.disabled,
        },
      }),
    },
    {
      props: { variant: 'navigation' },
      style: ({ theme }) => ({
        color: theme.palette.primary.main,
        '&:hover': {
          color: theme.palette.primary.dark,
          backgroundColor: 'transparent',
        },
      }),
    },
  ],
};