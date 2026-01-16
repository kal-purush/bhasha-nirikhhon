import { ConnectedTvOutlined } from "@mui/icons-material";
import {
  ADD_ENTRY,
  EDIT_ENTRY,
  DELETE_ENTRY,
  RESET_FORM_DATA,
  SUBMIT_FORM,
} from "./entryActionTypes";

export const addEntry = (name, value) => {
  // return (dispatch, getState) => {
  //   const { formData } = getState().entry;
  //   dispatch({
  //     type: ADD_ENTRY,
  //     payload: {
  //       ...formData,
  //       [name]: value
  //     },
  //   });
  // };
  console.log(name, value, "/////.//////")
  return {
    type: ADD_ENTRY,
    payload: {name, value}
  }
};

export const submitForm = () => {
  return (dispatch, getState) => {
    const { formData } = getState().entry;
    // dispatch(addEntry(formData));
    dispatch(resetFormData());
  };
};

export const resetFormData = () => {
  return {
    type: RESET_FORM_DATA,
  };
};

export const handleModalView = () => {
  return (dispatch) => {
    dispatch({
      type: "IS_MODAL_OPEN",
    });
  };
};
export const editEntry = () => {
  return {
    type: EDIT_ENTRY,
  };
};
export const deleteEntry = (index) => {
  return (dispatch, getState) => {
    const { entries } = getState().entry;
    entries.splice(index, 1);
    // console.log(index, value, "ho gaya");
    // console.log(entries, "entries");
    dispatch({
      type: DELETE_ENTRY,
      payload: {
        ...entries,
        entries: entries,
      },
    });
  };
};