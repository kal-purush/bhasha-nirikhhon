import hasCompletedIncomeForm from './index'
import { updateHasCompletedIncomeForm } from '../../actions'
import { UpdateHasCompletedIncomeFormAction } from '../../types'

describe('hasCompletedIncomeForm', () => {
  describe('UPDATE_HAS_COMPLETED_INCOME_FORM', () => {
    it('updates hasCompletedIncomeForm', () => {
      const initialState: boolean = false
      const action: UpdateHasCompletedIncomeFormAction = updateHasCompletedIncomeForm(true)
      expect(hasCompletedIncomeForm(initialState, action)).toBe(true)
    })
  })
})