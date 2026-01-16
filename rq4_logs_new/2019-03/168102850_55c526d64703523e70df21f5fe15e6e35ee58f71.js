import firebase from 'firebase';
import { Alert } from 'react-native';
import { ATTENDANCEUPDATE, ATTENDANCECREATE, ATTENDANCEFETCH, ATTENDANCESAVE, ATTENDANCE_FETCH_LOAD_START, ATTENDANCE_FETCH_LOAD_END } from './types';
import ListAttendance from '../component/TimeLine/ListAttendance';

export const AttendanceUpdate = ({ name, value }) => {
    return {
        type: ATTENDANCEUPDATE,
        payload: { name, value }
    };
};

export const AttendanceCreate = ({ ChildName, gender, Dob, Regdate }) => {
    const { currentUser } = firebase.auth();
    return (dispatch) => {
        console.log(firebase.auth());
        firebase.database().ref(`/users/${currentUser.uid}/Timeline/Attendance`)
            .push({ ChildName, gender, Dob, Regdate })
            .then(() => {
                dispatch({
                    type: ATTENDANCECREATE
                });
                // ActionSheet.childList({ type: reset });
            })
            .catch((error) => {
                console.log(error);
            });
    };
};

export const AttendanceFetch = () => {
    const { currentUser } = firebase.auth();

    return (dispatch) => {
        fetchLoad(dispatch);
        firebase.database().ref(`/users/${currentUser.uid}/Timeline/Attendance`)
            .on('value', snapshot => {
                dispatch({ type: ATTENDANCEFETCH, payload: snapshot.val() });
                dispatch({ type: ATTENDANCE_FETCH_LOAD_END, payload: false });
            });
    };
};

export const AttendanceSave = ({ ChildName, gender, Dob, Regdate, uid }) => {
    const { currentUser } = firebase.auth();
    return (dispatch) => {
        firebase.database().ref(`/users/${currentUser.uid}/Timeline/Attendance/${uid}`)
            .set({ ChildName, gender, Dob, Regdate })
            .then(() => {
                dispatch({
                    type: ATTENDANCESAVE
                });
            });
    };
};

export const AttendanceDelete = ({ uid }, navigate) => {
    const { currentUser } = firebase.auth();
    return (dispatch) => {
        Alert.alert(
            'Need Attention',
            'Do you Want to Delete..',
            [
                {
                    text: 'Cancel', onPress: () =>
                        dispatch({
                            type: ListAttendance
                        }),
                    style: 'cancel',
                },
                {
                    text: 'OK', onPress: () =>
                        firebase.database().ref(`/users/${currentUser.uid}/Timeline/Attendance/${uid}`)
                            .remove()
                            .then(() => {
                                dispatch({
                                    type: ListAttendance
                                });
                                navigate.navigate('AttendanceTab');
                            })
                },
            ],
            { cancelable: false },
        );
    };
};

const fetchLoad = (dispatch) => {
    dispatch({ type: ATTENDANCE_FETCH_LOAD_START, payload: true });
};