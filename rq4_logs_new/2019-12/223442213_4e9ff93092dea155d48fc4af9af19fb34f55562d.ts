import { User } from '../interfaces/user';

export class AuthService {
  currentUser: User | null;
  callback: ((data: any) => void) | null;

  constructor() {
    // Get auth info from localStorage
    const storedAuth = localStorage ? localStorage.getItem('auth') : null;

    this.currentUser = storedAuth ? JSON.parse(storedAuth) : null;
    this.callback = null;
  }

  async login(data: User) {
    if (localStorage) {
      localStorage.setItem('auth', JSON.stringify(data));
    }
    this.currentUser = data;
    if (this.callback) {
      this.callback(data);
    }
  }

  logout() {
    this.currentUser = null;
    if (localStorage) {
      localStorage.clear();
    }
    if (this.callback) {
      this.callback(null);
    }
  }

  subscribe(callback: any) {
    this.callback = callback;
    return () => {
      this.callback = null;
    };
  }
}