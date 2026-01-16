// * STORE
import { addUser } from './Auth.store';

// * HELPERS
import { isEmpty, isvalidate } from '../../Helpers/validate';

export async function Login(user: string, password: string) {
  try {
    isEmpty(user, password);
    isvalidate(
      [user],
      /^[a-zA-Z0-9._%+-]+@gmail.com$/,
      'Debe ser un Correo de Gmail'
    );
    isvalidate(
      [password],
      /^(?=.*[A-Z])(?=.*\d).+$/,
      'Debe cumplir con los requisitos: tenga al menos una letra mayúscula y un número.'
    );

    const data = {
      User: user,
      Password: password,
    };

    return addUser(data);
  } catch (error: any) {
    throw new Error(error.message);
  }
}