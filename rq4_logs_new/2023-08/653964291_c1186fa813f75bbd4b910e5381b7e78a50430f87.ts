import {AxiosInstance} from 'axios';
import {createAsyncThunk} from '@reduxjs/toolkit';
import {AppDispatch, State} from '../types/state';
import {APIRoute, AppRoute} from '../constants';
import {saveToken} from '../services/token';
import {redirectToRoute} from './action';
import { User, UserGeneral, FileType, UserFullInfo, Friend } from '../types/user';
import { QuestionnaireCoach } from '../types/questionnaire';
import { adaptAvatarToServer, adaptCertificateToServer, adaptCoachToServer } from '../utils/adapters/adaptersToServer';
import { setAuthInfo, setUserFullInfo } from './user-process/user-process';
import { fetchUser } from './api-actions-user';

export const Action = {
  REGISTER_COACH: 'coach/register',
  UPD_CERTIFICATE: 'coach/updateCertificate',
  POST_CERTIFICATE: 'coach/postCertificate',
  DELETE_CERTIFICATE: 'coach/deleteCertificate',
  DELETE_COACH_FRIEND:  'coach/deleteCoachFriend'
};

export const registerCoach = createAsyncThunk<void, UserGeneral & QuestionnaireCoach & FileType, {
  dispatch: AppDispatch;
  state: State;
  extra: AxiosInstance;
 }>(
   Action.REGISTER_COACH,
   async (newCoach: UserGeneral & QuestionnaireCoach & FileType, { dispatch, extra: api }) => {
     const { data } = await api.post<{ id: string }>(APIRoute.Register, adaptCoachToServer(newCoach));

     const {data: {accessToken}} = await api.post<User>(APIRoute.Login, {email: newCoach.email, password: newCoach.password});
     saveToken(accessToken);
     if (data && newCoach.avatarImg?.name && newCoach.fileCertificate?.name) {
       const postAvatarApiRoute = `${APIRoute.Files}/avatar`;
       await api.post(postAvatarApiRoute, adaptAvatarToServer(newCoach.avatarImg));

       const postCertificateApiRoute = `${APIRoute.Files}/coach/certificate`;
       await api.post(postCertificateApiRoute, adaptCertificateToServer(newCoach.fileCertificate));
     }
     const userFullInfo = await api.get<UserFullInfo>(APIRoute.CheckUser);
     dispatch(setAuthInfo({authInfo: {id: data.id, userName: userFullInfo.data.userName, role: userFullInfo.data.role, email: userFullInfo.data.email , accessToken: accessToken}}));
     dispatch(setUserFullInfo({userFullInfo: userFullInfo.data}));
     dispatch(redirectToRoute(AppRoute.AccountCoach));
   });

export const updateCertificate = createAsyncThunk<void, FileType, {
    dispatch: AppDispatch;
    state: State;
    extra: AxiosInstance;
   }>(
     Action.UPD_CERTIFICATE,
     async ({certificateId, fileCertificate }, { extra: api }) => {
       if (fileCertificate) {
         const postCertificateApiRoute = `${APIRoute.Files}/coach/certificate/update`;
         await api.post(postCertificateApiRoute, adaptCertificateToServer(fileCertificate, certificateId));
       }

     });

export const postCertificate = createAsyncThunk<void, FileType, {
      dispatch: AppDispatch;
      state: State;
      extra: AxiosInstance;
     }>(
       Action.POST_CERTIFICATE,
       async (newCertificate: FileType, { dispatch, extra: api }) => {
         if (newCertificate.fileCertificate) {
           const postCertificateApiRoute = `${APIRoute.Files}/coach/certificate`;
           await api.post(postCertificateApiRoute, adaptCertificateToServer(newCertificate.fileCertificate));
           dispatch(fetchUser());
           dispatch(redirectToRoute(AppRoute.AccountCoach));
         }

       });

export const deleteCertificate = createAsyncThunk<void, string, {
        dispatch: AppDispatch;
        state: State;
        extra: AxiosInstance;
       }>(
         Action.DELETE_CERTIFICATE,
         async (certificateId, { dispatch, extra: api }) => {
           await api.delete(`${APIRoute.Coach}/certificate/delete/${certificateId}`);
           dispatch(fetchUser());
           dispatch(redirectToRoute(AppRoute.AccountCoach));
         });


export const deleteCoachFriend = createAsyncThunk<Friend, string, {
                  dispatch: AppDispatch;
                  state: State;
                  extra: AxiosInstance; }>(
                    Action.DELETE_COACH_FRIEND,
                    async (id, {extra: api}) => {
                      try {
                        const {data} = await api.post<Friend>(`${APIRoute.Coach}/friends/delete/${id}`);
                        return data;
                      } catch (error) {
                        return Promise.reject(error);
                      }
                    });

