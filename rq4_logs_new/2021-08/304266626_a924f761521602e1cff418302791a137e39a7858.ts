import { createUseStyles } from 'react-jss';

import { Theme } from 'src/common';

export const useMainEconomyStyles = createUseStyles(
  (theme: Theme) => ({
    root: {
      display: 'grid',
      gridGap: 20,

      [theme.breakpoints.lg()]: {
        gridGap: 40,
        gridTemplateColumns: '1fr 1fr'
      }
    },

    icon: {
      marginLeft: 10,
      marginRight: 4
    },

    scheme: {
      [theme.breakpoints.lg()]: {
        gridColumnStart: 1,
        gridColumnEnd: 3
      }
    },

    card: {
      display: 'flex',
      flexDirection: 'column',
      alignItems: 'flex-start',
      minHeight: 320,
      padding: '32px 40px',

      [theme.breakpoints.lg()]: {
        padding: '64px 80px'
      }
    },

    cardText: {
      marginBottom: 'auto'
    }
  }),
  {
    name: 'MainEconomy'
  }
);