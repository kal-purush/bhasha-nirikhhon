import { connect } from 'react-redux'
import UserManagement from './../components/UserManagement/UserManagement'
import {getUsersRequest, addUserRequest} from './../actions/user'

const mapStateToProps = (state) => {
  return {
    users: state.users
  }
}

const UserManagementContainer = connect(
  mapStateToProps,
  {getUsers: getUsersRequest, addUser: addUserRequest}
)(UserManagement)

export default UserManagementContainer