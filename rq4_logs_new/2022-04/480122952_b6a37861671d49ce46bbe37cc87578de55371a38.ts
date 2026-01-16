import { api } from "../../../Services/api";
import { deleteUser, registerUser, updateUser } from "./actions";

interface Users {
  id?: number;
  name?: String | undefined;
  email?: String;
  phone?: String;
}

export const addUserThunk = (data: Users) => (dispatch: any) => {
  api
    .post(`/users`, data)
    .then((response) => {
      dispatch(registerUser(response.data));
      console.log("deu bom");
    })
    .catch((_) => {
      console.log("error");
    });
};

export const UpdateUserThunk = (data: Users, id: number) => (dispatch: any) => {
  api
    .patch(`/users/${id}`, data)
    .then((response) => {
      dispatch(updateUser(response.data));
      console.log("deu bom");
    })
    .catch((_) => {
      console.log("error");
    });
};

export const DeleteUserThunk = (idUser: number) => (dispatch: any) => {
  api
    .delete(`/users/${idUser}`)
    .then((response) => {
      dispatch(deleteUser(idUser));
      console.log("deu bom");
    })
    .catch((_) => {
      console.log("error");
    });
};