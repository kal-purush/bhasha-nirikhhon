// Define types for the state and action payloads
export interface AuthState {
    isAuthenticated: boolean;
    token : string | null;
    user: User | null;
    status: 'idle' | 'loading' | 'succeeded' | 'failed';
    error: string | null;
  }
  
  export interface User {
    id: string;
    firstName: string;
    lastName: string;
    email: string;
  }
  
  export interface Credentials {
    email: string;
    password: string;
  }
  
  export interface UserInfo {
    firstName: string;
    lastName: string;
    email: string;
    password: string;
  }
  
  export interface ApiResponse {
    data: User;
    token: string;
  }
  