import reducer, {
  setIsEditMode,
  calendarSelectors,
  setSelectedDay,
} from '../../ducks/calendar/CalendarReducer';
import { RootState } from '../../ducks';

describe('CalendarReducer', () => {
  describe('reducer, actions and selectors', () => {
    const initialState = {
      selectedDay: null,
      isEditMode: false,
    };

    it('should return the initial state on first run', () => {
      const result = reducer(undefined, {} as any);
      expect(result).toEqual(initialState);
    });

    it('should set isEditMode correctly', () => {
      const isEditMode = true;

      const result = reducer(initialState, setIsEditMode(isEditMode));
      const rootState = { calendar: result };
      expect(calendarSelectors.selectIsEditMode(rootState as RootState)).toBe(
        isEditMode
      );
    });

    it('should set the selected Day correctly', () => {
      const selectedDay = '01-01-20';

      const result = reducer(initialState, setSelectedDay(selectedDay));
      const rootState = { calendar: result };
      expect(
        calendarSelectors.selectedSelectedDay(rootState as RootState)
      ).toBe(selectedDay);
    });
  });
});