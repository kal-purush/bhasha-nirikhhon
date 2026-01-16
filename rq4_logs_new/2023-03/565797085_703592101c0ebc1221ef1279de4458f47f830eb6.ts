import { Theme } from '@mui/material'

export const getComponentOverrides = (theme: Theme): Theme['components'] => ({
  MuiFormLabel: {
    styleOverrides: {
      root: {
        // color: '#5f6368',
        fontWeight: 500
        // fontSize: 13
      }
    }
  },
  MuiFormHelperText: {
    styleOverrides: {
      root: {
        // color: '#5f6368',
        fontWeight: 500,
        fontSize: 11
      }
    }
  },
  MuiInputBase: {
    styleOverrides: {
      sizeSmall: {
        height: '36px'
      }
    }
  },
  MuiIconButton: {
    styleOverrides: {
      root: {
        padding: 4
      }
    }
  },
  // MuiSvgIcon: {
  //   styleOverrides: {
  //     root: {
  //       color: theme.palette.primary.lighter
  //       // fontSize: '1.25rem'
  //     }
  //   }
  // },
  MuiPaper: {
    styleOverrides: {
      root: {
        backgroundImage: 'none'
      }
    }
  },
  MuiButton: {
    styleOverrides: {
      root: {
        fontWeight: 500,
        textTransform: 'initial'
      }
    }
  }
})