export class Authentication {
  private static authenticated = false;
  static authenticateLogin(loginData: LoginData): any {
    return fetch("/api/login", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(loginData),
    }).then((response) => response.json());
  }

  static authenticateJwt(): any {
    return fetch("/auth", {
      method: "GET",
      headers: { Authorization: localStorage.getItem("token") as string },
    }).then((response) => response.json());
  }

  static authenticate(setAuth: (bool: boolean) => void): void {
    if (!Authentication.getAuthenticated()) {
      (async () => {
        const response = await Authentication.authenticateJwt();
        if (response.code == 201) {
          Authentication.setAuthenticated(true);
        }
        setAuth(Authentication.getAuthenticated());
      })();
    }
  }
  static setAuthenticated(bool: boolean) {
    this.authenticated = bool;
  }

  static getAuthenticated() {
    return this.authenticated;
  }
}

export interface LoginData {
  username: string;
  password: string;
}