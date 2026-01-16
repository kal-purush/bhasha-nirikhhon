import { connect } from 'react-redux';
import { setStatusBarDark, setStatusBarLight } from '../../components/StatusBar/action';
import LoginScreen from './';

const mapStateToProps = () => ({});

const mapDispatchToProps = (dispatch: any) => ({
  setStatusBarDark: () => dispatch(setStatusBarDark()),
  setStatusBarLight: () => dispatch(setStatusBarLight())
});

export default connect(mapStateToProps, mapDispatchToProps)(LoginScreen);