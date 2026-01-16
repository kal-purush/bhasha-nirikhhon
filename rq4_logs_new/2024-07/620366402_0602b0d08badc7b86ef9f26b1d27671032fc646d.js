export const FETCH_TANK_RECONCILIATION_SUCCESS = 'FETCH_TANK_RECONCILIATION_SUCCESS';
export const FETCH_TANK_RECONCILIATION_FAILURE = 'FETCH_TANK_RECONCILIATION_FAILURE';
export const FETCH_TANK_RECONCILIATION_BY_SITE_SUCCESS = 'FETCH_TANK_RECONCILIATION_BY_SITE_SUCCESS';
export const FETCH_TANK_RECONCILIATION_BY_SITE_FAILURE = 'FETCH_TANK_RECONCILIATION_BY_SITE_FAILURE';

export const fetchTankReconciliation = (date) => async (dispatch) => {
  try {
    const response = await axiosInstance.get(`/tankreconciliation?date=${date}`);
    dispatch({ type: FETCH_TANK_RECONCILIATION_SUCCESS, payload: response.data });
  } catch (error) {
    dispatch({ type: FETCH_TANK_RECONCILIATION_FAILURE, payload: error.message });
  }
};

export const fetchTankReconciliationBySite = (date, siteId) => async (dispatch) => {
  try {
    const response = await axiosInstance.get(`/tankreconciliation/by-site?date=${date}&siteId=${siteId}`);
    dispatch({ type: FETCH_TANK_RECONCILIATION_BY_SITE_SUCCESS, payload: response.data });
  } catch (error) {
    dispatch({ type: FETCH_TANK_RECONCILIATION_BY_SITE_FAILURE, payload: error.message });
  }
};