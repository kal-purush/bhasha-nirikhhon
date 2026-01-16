import { makeStyles } from 'tss-react/mui';
import type { ThemePalette } from '@weavcraft/types';

export const useDisplayStyles = makeStyles<ThemePalette>({
  name: 'PaletteDisplay',
})((theme, { background, divider, text }) => ({
  root: {
    position: 'relative',
    height: theme.spacing(40),

    [theme.breakpoints.only('xs')]: {
      fontSize: '0.7em',
      height: theme.spacing(32),
    },
    '& > *.MuiGrid-item': {
      display: 'flex',
      flexDirection: 'column',
      justifyContent: 'flex-start',
      padding: theme.spacing(1),
      fontWeight: 600,
      fontSize: '1.125em',

      '&:first-child': {
        alignItems: 'flex-start',
        background: background.default,
        borderRight: `1px solid ${divider}`,
        color: text.primary,
      },
      '&:last-child': {
        alignItems: 'flex-end',
        background: background.paper,
        borderLeft: `1px solid ${divider}`,
        color: text.secondary,
      },
    },
  },
  colors: {
    position: 'absolute',
    height: 80,
    width: '80%',
    top: theme.spacing(10),
    left: '50%',
    transform: 'translateX(-50%)',
    gap: theme.spacing(1),

    [theme.breakpoints.only('xs')]: {
      top: theme.spacing(7),
    },
    '& > *': {
      display: 'flex',
      flexDirection: 'row',
      alignItems: 'center',
      justifyContent: 'space-between',
      padding: theme.spacing(0.5, 1.5),
      border: `1px solid ${divider}`,
      borderRadius: `${theme.spacing(1.6)} / 50%`,
      fontWeight: 500,
      fontSize: '0.8em',
    },
  },
}));