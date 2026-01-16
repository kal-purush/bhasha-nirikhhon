import { makeStyles, Theme } from '@material-ui/core/styles';

const useNavbarStyles = makeStyles((theme: Theme) => ({
  title: {
    textTransform: 'none',
    fontWeight: 'bold',
    lineHeight: '100%',
    textDecoration: 'underline',
    marginRight: theme.spacing(1),
  },
  userNameButton: {
    padding: 0,
    height: '100%',
    marginLeft: theme.spacing(8),
    marginRight: theme.spacing(2),
  },
  userListWrapper: {
    display: 'flex',
    flex: 1,
    marginLeft: theme.spacing(8),
  },
}));

export default useNavbarStyles;