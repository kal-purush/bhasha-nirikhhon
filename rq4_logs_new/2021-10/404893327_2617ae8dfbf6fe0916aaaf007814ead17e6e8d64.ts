import { makeStyles } from '@material-ui/core/styles';
const useStyles = makeStyles((theme) => ({
  profileContainer: {
    margin: theme.spacing(4),
    height: '900px',
    maxWidth: '760px',
  },
  coverImage: {
    maxHeight: '260px',
    width: '100%',
    borderRadius: theme.shape.borderRadius,
  },
  basicInfoContainer: {
    position: 'relative',
    bottom: '94px',
  },
  profilePic: {
    borderRadius: '50%',
    maxWidth: '180px',
    height: '180px',
    width: 'auto',
    border: '2px solid white',
    boxShadow: '-1px 9px 11px -5px rgb(0 0 0 / 35%)',
  },
  locationIcon: {
    position: 'relative',
    top: '5px',
  },
  aboutContainer: {
    margin: theme.spacing(4),
    position: 'relative',
    bottom: '90px',
  },
  description: {
    maxWidth: '680px',
    marginTop: theme.spacing(1),
    marginBottom: theme.spacing(1),
  },
  galleryContainer: {
    flexDirection: 'row',
  },
  galleryPic: {
    margin: theme.spacing(1),
    height: '180px',
    width: '180px',
    objectFit: 'cover',
  },
  bookingContainer: {
    margin: theme.spacing(4),
    width: '400px',
    height: '500px',
  },
  dateContainer: {
    justifyContent: 'space-between',
  },
  requestContainer: {
    padding: theme.spacing(4),
    justifyContent: 'space-evenly',
    alignItems: 'center',
    height: '100%',
  },
}));

export default useStyles;