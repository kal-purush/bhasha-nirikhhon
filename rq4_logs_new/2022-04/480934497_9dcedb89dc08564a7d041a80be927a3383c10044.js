import axios from "axios";
import { historyUrl } from "../utils/index";
import { historyConstants } from "../utils/dataActions";

const {
  GET_HISTORY,
  CLEAR_HISTORY,
  ADD_VIDEO_TO_HISTORY,
  DELETE_VIDEO_FROM_HISTORY,
} = historyConstants;

const config = {
  headers: { authorization: localStorage.getItem("token") },
};

export const getHistory = async (dispatch) => {
  try {
    const response = await axios.get(historyUrl, config);
    if (response.status === 200) {
      dispatch({ type: GET_HISTORY, payload: response.data.history });
    }
  } catch (e) {
    console.log(e);
  }
};

export const addVideoToHistory = async (video, dispatch) => {
  console.log("services");
  try {
    const response = await axios.post(historyUrl, { video }, config);

    if (response.status === 201) {
      dispatch({ type: ADD_VIDEO_TO_HISTORY, payload: response.data.history });
      console.log(response.data.history);
    }
  } catch (e) {
    console.log(e);
  }
};

export const deleteVideoFromHistory = async (videoID, dispatch) => {
  try {
    const response = await axios.delete(`${historyUrl}/${videoID}`, config);
    if (response.status === 200) {
      dispatch({
        type: DELETE_VIDEO_FROM_HISTORY,
        payload: response.data.history,
      });
    }
  } catch (e) {
    console.log(e);
  }
};

export const clearHistory = async (dispatch) => {
  try {
    const response = await axios.delete(`${historyUrl}/all`, config);
    if (response.status === 200) {
      dispatch({ type: CLEAR_HISTORY, payload: response.data.history });
    }
  } catch (e) {
    console.log(e);
  }
};