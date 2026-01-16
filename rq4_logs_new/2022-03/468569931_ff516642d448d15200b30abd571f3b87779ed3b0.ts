
import {
  createTheme,
  responsiveFontSizes,
  ThemeOptions,
} from '@mui/material/styles';
import { defaultShadows } from './shadows';

const baseOptions = {
  direction: 'ltr',
  components: {
    MuiAvatar: {
      styleOverrides: {
        fallback: {
          height: '75%',
          width: '75%',
        },
      },
    },
    MuiInputBase: {
      styleOverrides: {
        input: {
          '&::placeholder': {
            opacity: 0.86,
            color: '#42526e',
          },
        },
      },
    },
    MuiButton: {
      styleOverrides: {
        sizeSmall: {
          fontSize: 12,
          lineHeight: '16px',
          padding: '12px 30px',
        },
        sizeMedium: {
          fontSize: 15,
          lineHeight: '16px',
          padding: '16px 30px',
        },
        sizeLarge: {
          fontSize: 18,
          lineHeight: '16px',
          padding: '24px 30px',
        },
      },
    },
    MuiCssBaseline: {
      styleOverrides: {
        '*': {
          boxSizing: 'border-box',
        },
        html: {
          MozOsxFontSmoothing: 'grayscale',
          WebkitFontSmoothing: 'antialiased',
          height: '100%',
          width: '100%',
        },
        body: {
          height: '100%'
        },
        '#root': {
          height: '100%',
        },
        '#nprogress .bar': {
          zIndex: '2000 !important',
        },
      },
    },
    MuiCardHeader: {
      defaultProps: {
        titleTypographyProps: {
          variant: 'h6',
        },
      },
    },
    MuiLinearProgress: {
      styleOverrides: {
        root: {
          borderRadius: 3,
          overflow: 'hidden',
        },
      },
    },
    MuiListItemIcon: {
      styleOverrides: {
        root: {
          minWidth: 'auto',
          marginRight: '16px',
        },
      },
    },
    MuiPaper: {
      styleOverrides: {
        root: {
          backgroundImage: 'none',
        },
      },
    },
  },
  typography: {
    button: {
      fontWeight: 600,
    },
    fontFamily:
      '-apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif, "Apple Color Emoji", "Segoe UI Emoji"',
    h1: {
      fontWeight: 600,
      fontSize: '3.5rem',
    },
    h2: {
      fontWeight: 600,
      fontSize: '3rem',
    },
    h3: {
      fontWeight: 600,
      fontSize: '2.25rem',
    },
    h4: {
      fontWeight: 600,
      fontSize: '2rem',
    },
    h5: {
      fontWeight: 600,
      fontSize: '1.5rem',
    },
    h6: {
      fontWeight: 600,
      fontSize: '1.125rem',
    },
    overline: {
      fontWeight: 600,
    },
  },
  shape: {
    borderRadius: 8,
  },
  palette: {
    // MUI default action configuration
    action: {
      active: 'rgba(0,0,0,0.54)',
      hover: 'rgba(0,0,0,0.04)',
      hoverOpacity: 0.04,
      selected: 'rgba(0,0,0,0.08)',
      selectedOpacity: 0.08,
      disabled: 'rgba(0,0,0,0.26)',
      disabledOpacity: 0.38,
      disabledBackground: 'rgba(0,0,0,0.12)',
      focus: 'rgba(0,0,0,0.12)',
      focusOpacity: 0.12,
      activatedOpacity: 0.12,
    },
    background: {
      default: '#E5E5E5',
      paper: '#ffffff',
    },
    error: {
      contrastText: '#ffffff',
      main: '#f44336',
    },
    mode: 'light',
    primary: {
      contrastText: '#ffffff',
      main: '#3C6670',
    },
    secondary: {
      contrastText: '#ffffff',
      main: '#1E1A34',
    },
    tertiary: {
      contrastText: '#181A1B',
      main: '#F1B434',
    },
    success: {
      contrastText: '#ffffff',
      main: '#4caf50',
    },
    text: {
      primary: '#181A1B',
      secondary: '#434A60',
    },
    warning: {
      contrastText: '#ffffff',
      main: '#ff9800',
    },
    divider: '#C7C6CC',
  },
  shadows: defaultShadows,
} as ThemeOptions;

export const createCustomTheme = (config = { responsiveFontSizes: true }) => {
  const themeOptions = baseOptions;

  let theme = createTheme(themeOptions);

  if (config.responsiveFontSizes) {
    theme = responsiveFontSizes(theme);
  }

  return theme;
};