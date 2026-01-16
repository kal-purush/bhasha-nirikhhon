// contains types and interfaces used by different components and files in the application

export type MapIndex = string | number;

export interface Toy {
  id?: string;
  _id?: string; // sometimes, the backend will return a different form of ID and in some cases it is difficult to change the parameter
  name: string;
  // string is returned without .populate(), but the complete user document is returned with .populate()
  user: string | User;
  manufacturer?: string;
  date_posted: Date;
  description?: string;
  image_url?: string;
  franchise?: string;
  series?: string;
}

export interface User {
  id?: string;
  _id?: string; // sometimes, the backend will return a different form of ID and in some cases it is difficult to change the parameter
  username: string;
  //TODO: I think this can be changed to just required instead of optional
  toysOwned?: Toy[];
  registerDate: Date;
}

//TODO: this kind of makes no sense, please fix the typing here
export interface ErrorObject {
  // technically it is either a string or an object that contains a message property that is a string
  message: any;
  status: string | null;
  id: null | string;
}

export interface AuthStateObject {
  isSignedIn: boolean;
  user: User | null;
  isLoading: boolean;
}