import { ORGANIZATION, functionsZone } from "@/config/envInfo";

import { NotifVariant, Action } from "@/enums/store";

import { FirestoreThunk } from "@/types/store";

import { enqueueSnackbar, showErrSnackbar } from "@/store/actions/appActions";

/**
 * Creates firestore async thunk:
 * - dispatches signout request to firestore
 * - enqueues success/error snackbar depending on the outcome of firestore operation
 * @returns async thunk
 */
export const signOut = (): FirestoreThunk => async (
  dispatch,
  getState,
  { getFirebase }
) => {
  const firebase = getFirebase();
  try {
    await firebase.auth().signOut();

    dispatch(
      enqueueSnackbar({
        key: new Date().getTime() + Math.random(),
        message: "Hai effettuato il logout",
        options: {
          variant: NotifVariant.Success,
        },
      })
    );
  } catch (err) {
    dispatch(
      enqueueSnackbar({
        key: new Date().getTime() + Math.random(),
        message: "Si è verificato un errore",
        options: {
          variant: NotifVariant.Error,
        },
      })
    );
  }
};

/**
 * Creates firestore async thunk:
 * - sends a query to firestore regarding admin status of the user
 * - on success updates admin status in local store
 * - on error enqueues error snackbar
 * @returns async thunk
 */
export const queryUserAdminStatus = (): FirestoreThunk => async (
  dispatch,
  getState,
  { getFirebase }
) => {
  try {
    const firebase = getFirebase();
    const resp = await firebase
      .app()
      .functions(functionsZone)
      .httpsCallable("amIAdmin")({
      organization: ORGANIZATION,
    });

    const auth = getState().firebase.auth;

    if (auth.uid) {
      dispatch({
        type: Action.IsAdminReceived,
        payload: { uid: auth.uid, amIAdmin: resp.data.amIAdmin },
      });
    }
  } catch (err) {
    showErrSnackbar();
  }
};