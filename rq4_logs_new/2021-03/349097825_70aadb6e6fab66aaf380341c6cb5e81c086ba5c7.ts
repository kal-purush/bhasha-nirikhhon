import { createStyles, Theme } from '@material-ui/core/styles';

const styles = (theme: Theme) =>
  createStyles({
    item: {
      display: 'flex',
      flexDirection: 'column',
      justifyContent: 'center',
      alignItems: 'center',
      textAlign: 'justify',
    },
    title: {
      display: 'flex',
      flexDirection: 'column',
      justifyContent: 'center',
      alignItems: 'center',
      margin: '4%',
      '& a': {
        marginTop: '8px',
        'font-size': '20px',
        color: theme.palette.text.secondary,
      },
      '& div': {
        width: '100px',
        height: '100px',
      },
      '& a img': {
        color: theme.palette.text.secondary,
        margin: '10%',
      },
    },
    divider: {
      height: '2px',
      margin: '4.5% 5% 0',
    },
  });

export default styles;