import axios from 'axios';

import {
  FETCH_WINES_FULFILLED,
  FETCH_WINES_REJECTED,
  FETCH_WINES_NO_MATCH,
  FETCH_WINES,
  REMOVE_FROM_CELLAR_FULFILLED,
  REMOVE_FROM_CELLAR_REJECTED,
  REMOVE_FROM_CELLAR,
  TOGGLE_DETAILED_RESULT_VIEW,
  FETCH_REVIEWS_FULFILLED,
  FETCH_REVIEWS_REJECTED,
  FETCH_REVIEWS_NO_MATCH,
  FETCH_REVIEWS,
} from './constants';

const fetchClickedReview = (query, dispatch) => {
  axios.get(`/api/${query}`)
  .then((response) => {
    if (response.data.data && response.data.data.length > 0) {
      dispatch({ type: FETCH_REVIEWS_FULFILLED, payload: response.data });
    } else {
      dispatch({ type: FETCH_REVIEWS_NO_MATCH, payload: response.data });
    }
  })
  .catch((err) => {
    dispatch({ type: FETCH_REVIEWS_REJECTED, payload: err });
  });
};

export const loadClickedReview = item => (dispatch, getState) => {
  dispatch({ type: FETCH_REVIEWS });
  if (!item.table) {
    fetchClickedReview('getAllReviews', dispatch, getState);
  } else if (item.table === 'wine') {
    fetchClickedReview(`getWineByProperty?property=${item.property}&value=${item.value}&table=${item.table}`, dispatch, getState);
  } else {
    fetchClickedReview(`getWineByForeignProperty?property=${item.property}&value=${item.value}&table=${item.table}`, dispatch, getState);
  }
};

export const loadOrderedClickedReview = item => (dispatch, getState) => {
  dispatch({ type: FETCH_REVIEWS });
  if (!item.table) {
    fetchClickedReview(`getAllReviews?orderedProp=${item.orderedProp}`, dispatch, getState);
  } else if (item.table === 'wine') {
    fetchClickedReview(`getWineByProperty?property=${item.property}&value=${item.value}&table=${item.table}&orderedProp=${item.orderedProp}`, dispatch, getState);
  } else {
    fetchClickedReview(`getWineByForeignProperty?property=${item.property}&value=${item.value}&table=${item.table}&orderedProp=${item.orderedProp}`, dispatch, getState);
  }
};

export const loadOrderedCellarClickedWine = item => (dispatch, getState) => {
  dispatch({ type: FETCH_REVIEWS });
  if (!item.table) {
    fetchClickedWine(`getAllCellar?orderedProp=${item.orderedProp}`, dispatch, getState);
  } else if (item.table === 'wine') {
    fetchClickedWine(`getWineByProperty?property=${item.property}&value=${item.value}&table=${item.table}&orderedProp=${item.orderedProp}`, dispatch, getState);
  } else {
    fetchClickedWine(`getWineByForeignProperty?property=${item.property}&value=${item.value}&table=${item.table}&orderedProp=${item.orderedProp}`, dispatch, getState);
  }
};

const removeFromCellar = (query, dispatch) => {
  axios.get(`/api/${query}`)
  .then((response) => {
    dispatch({ type: REMOVE_FROM_CELLAR_FULFILLED, payload: response.data });
  })
  .catch((err) => {
    dispatch({ type: REMOVE_FROM_CELLAR_REJECTED, payload: err });
  });
};

const fetchClickedWine = (query, dispatch) => {
  axios.get(`/api/${query}`)
  .then((response) => {
    if (response.data.data && response.data.data.length > 0) {
      dispatch({ type: FETCH_WINES_FULFILLED, payload: response.data });
    } else {
      dispatch({ type: FETCH_WINES_NO_MATCH, data: response.data });
    }
  })
  .catch((err) => {
    dispatch({ type: FETCH_WINES_REJECTED, payload: err });
  });
};

export const loadCellar = () => (dispatch) => {
  dispatch({ type: FETCH_WINES });
  fetchClickedWine('getAllCellar', dispatch);
};

export const loadCellarOrdered = item => (dispatch) => {
  dispatch({ type: FETCH_WINES });
  fetchClickedWine(`getAllCellar?orderedProp=${item.orderedProp}`, dispatch);
};

export const removeWineFromCellar = id => (dispatch) => {
  dispatch({ type: REMOVE_FROM_CELLAR });
  removeFromCellar(`removeFromCellar?id=${id}`, dispatch);
};

export const toggleDetailedView = id => (dispatch) => {
  dispatch({ type: TOGGLE_DETAILED_RESULT_VIEW });
};