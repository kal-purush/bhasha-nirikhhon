import { connect } from 'react-redux';
import Welcome from './welcome';

const mapStateToProps = ({ session }) => ({
  currentUser: session.currentUser,
});

export default connect(mapStateToProps)(Welcome);