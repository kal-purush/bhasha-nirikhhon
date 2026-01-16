export interface VideoItem {
  author: string;
  description: string;
  videoURL: string;
  thumbnailURL: string;
  videoType: string; // example: video/mp4
  validated: boolean;
  isAnswer: boolean;
}

export interface Authenticate {
  user: string;
  pass: string;
}

export interface AuthenticateResponse {
  jwt: string;
  nom: string;
  cognoms: string;
  imatgePerfil: string;
}

export interface SignUp {
  username: string,
  password: string,
  email: string,
  nom: string,
  cognoms: string
}

export interface SignUpResponse {
  id: number,
  username: string,
  email: string,
  nom: string,
  cognoms: string
}