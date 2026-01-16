import { fromJS } from 'immutable';

import * as actions from './actions';
import { FETCH_ALL_RECORDS_SUCCEEDED, SORT_RECORDS } from './action-types';

import { Actions } from '../../../types';
import { SortOrder } from '../../../types/enums';

const initialState = fromJS({
  records: []
});

export default function reducer(
  state = initialState,
  action: Actions<typeof actions>
) {
  switch (action.type) {
    case FETCH_ALL_RECORDS_SUCCEEDED:
      return state.set('records', fromJS(action.payload.records));
    case SORT_RECORDS:
      return state.update('records', (records: any) =>
        records.sort(
          (a: any, b: any) =>
            a
              .getIn(action.payload.field)
              .localeCompare(b.getIn(action.payload.field)) *
            (action.payload.order === SortOrder.DSC ? 1 : -1)
        )
      );
    default:
      return state;
  }
}