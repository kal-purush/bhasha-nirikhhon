import React from 'react';
import { connect } from 'react-redux'
import * as selectors from '../selectors'
import App from '../components/App'

const mapStateToProps = (state, ownProps) => {
  const filter = ownProps.params.activityType
  const filteredActivities = selectors.getActivitiesByFilter(state.activities, filter)
  return {
    transaction: state.transaction,
    activityType: filter,
    totalAmount: selectors.getTotal(filteredActivities)
  }
}

const mapDispatchToProps = (dispatch) => {
  return {
    clearTransactionId: ()=> dispatch({type:'CLEAR_TRANSACTION_ID'})
  }
}


export default connect(mapStateToProps, mapDispatchToProps)(App)