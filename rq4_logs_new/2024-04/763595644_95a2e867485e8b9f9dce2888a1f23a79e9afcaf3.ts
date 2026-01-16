import { makeStyles } from 'tss-react/mui';

export const useMainStyles = makeStyles({ name: 'Intro' })((theme) => ({
  root: {
    display: 'flex',
    flexDirection: 'column',
    gap: theme.spacing(3),

    '& img': {
      borderRadius: theme.spacing(2),
      transform: 'scale(0.8)',
      width: '100%',
    },
    '& .MuiTypography-root': {
      whiteSpace: 'pre-line',
    },
  },
  paragraph: {
    '& + *': {
      marginTop: theme.spacing(6),
    },
    '& > *': {
      display: 'flex',
      alignItems: 'center',
    },
  },
}));