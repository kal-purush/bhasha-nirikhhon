import {
  UPDATE_TOTAL_HOURS_WORKED,
  ReduxAction,
} from '../../types'
import {
  calculateTotalHoursWorked,
} from '../../../services/hours-calculations'

const totalHoursWorked = (state: number = 0, action: ReduxAction): number => {
  switch (action.type) {
    case UPDATE_TOTAL_HOURS_WORKED:
      const {
        grossRegularTimePay,
        grossOvertimePay,
      } = action.payload
      return calculateTotalHoursWorked(grossRegularTimePay, grossOvertimePay)
    default:
      return state
  }
}

export default totalHoursWorked