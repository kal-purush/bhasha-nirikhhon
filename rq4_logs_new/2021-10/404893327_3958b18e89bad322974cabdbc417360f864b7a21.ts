import { makeStyles } from '@material-ui/core';
import Image from '../../Images/cover.jpg';

const useStyles = makeStyles((theme) => ({
  container: {
    position: 'absolute',
    top: 0,
    minHeight: '100vh',
    backgroundColor: theme.palette.grey[100],
  },
  intro: {
    width: '50%',
    minHeight: '100vh',
    padding: theme.spacing(6),
    [theme.breakpoints.down('sm')]: {
      width: '100%',
    },
  },
  introText: {
    fontWeight: theme.typography.fontWeightBold,
  },
  image: {
    width: '50%',
    minHeight: '100vh',
    backgroundImage: `url(${Image})`,
    backgroundRepeat: 'no-repeat',
    backgroundSize: 'cover',
    backgroundPosition: 'center',
    [theme.breakpoints.down('sm')]: {
      width: '100%',
    },
  },
  form: {
    width: '80%', // Fix IE 11 issue.
    marginTop: theme.spacing(1),
  },
  label: {
    fontWeight: theme.typography.fontWeightBold,
    textTransform: 'uppercase',
    color: 'black',
  },
  textField: {
    paddingTop: '0.6rem',
  },
  input: {
    border: `2px solid ${theme.palette.grey[400]}`,
    borderRadius: theme.shape.borderRadius,
    padding: '1rem 0.5rem',
  },
  rightBorder: {
    borderRadius: `0px 5px 5px 0px`,
    borderWidth: `2px 2px 2px 1px`,
    width: '180px !important',
  },
  leftBorder: {
    borderRadius: `5px 0px 0px 5px`,
    borderWidth: `2px 1px 2px 2px`,
    width: '180px !important',
  },
  address: {
    width: '100%',
  },
  picker: {
    marginTop: theme.spacing(2),
    padding: '1rem 0',
  },
  button: {
    padding: '1.5rem 3rem',
    marginTop: theme.spacing(4),
    marginBottom: theme.spacing(2),
    [theme.breakpoints.down('sm')]: {
      padding: '1.5rem 2rem',
    },
  },
  buttonText: {
    fontWeight: theme.typography.fontWeightBold,
    textTransform: 'uppercase',
  },
}));

export default useStyles;