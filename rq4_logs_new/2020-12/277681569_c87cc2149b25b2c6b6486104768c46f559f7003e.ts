import { makeStyles } from '@material-ui/core';

const useAllCalendarStyles = makeStyles((theme) => ({
  container: {
    display: 'flex',
    flex: 0,
    padding: theme.spacing(2),
    justifyContent: 'center',
    alignItem: 'center',
    width: '340px',
    backgroundColor: theme.palette.background.paper,
  },
  leftCircleBase: {
    width: '50%',
    backgroundColor: '#d7e7ff',
  },
  leftCircleCompare: {
    width: '50%',
    backgroundColor: '#d3d19d',
  },
  rigthCircleBase: {
    background: `linear-gradient(to left,#d7e7ff 50%, ${theme.palette.background.paper} 50%)`,
  },
  rigthCircleCompare: {
    background: `linear-gradient(to left,#d3d19d 50%, ${theme.palette.background.paper} 50%)`,
  },
  rangeDayBase: {
    backgroundColor: '#d7e7ff',
  },
  rangeDayCompare: {
    backgroundColor: '#d3d19d',
  },
  selectedDayBase: {
    backgroundColor: '#3a86ff',
  },
  selectedDayCompare: {
    backgroundColor: '#d3d19d',
  },
  hasStreamDayDotContainer: {
    position: 'relative',
  },
  hasStreamDayDotBase: {
    position: 'absolute',
    height: 0,
    width: 0,
    border: '3px solid',
    borderRadius: 5,
    borderColor: '#3a86ff',
    right: '44%',
    transform: 'translateX(1px)',
    top: '80%',
    backGroundColor: '#3a86ff',
  },
  hasStreamDayDotCompare: {
    position: 'absolute',
    height: 0,
    width: 0,
    border: '3px solid',
    borderRadius: 5,
    borderColor: '#b1ae71',
    right: '44%',
    transform: 'translateX(1px)',
    top: '80%',
    backGroundColor: '#b1ae71',
  },
}));

export default useAllCalendarStyles;