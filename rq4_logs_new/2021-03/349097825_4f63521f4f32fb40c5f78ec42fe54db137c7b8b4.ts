import { createStyles, Theme } from '@material-ui/core/styles';

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
const styles = (theme: Theme) =>
  createStyles({
    root: {
      padding: theme.spacing(1),
      margin: theme.spacing(2, 1),
      background: theme.palette.background.default,
    },
    wordImage: {
      width: theme.spacing(40),
      hight: theme.spacing(22),
      margin: theme.spacing(1),
      borderRadius: '2px',
      boxShadow: theme.shadows[1],
      lineHeight: '0',
    },
    image: {
      width: '100%',
      objectFit: 'cover',
      objectPosition: 'center',
      borderRadius: 'inherit',
    },
    text: {
      userSelect: 'none',
    },
  });

export default styles;