class UserDetails {
  static getUsername(): string {
    return JSON.parse(sessionStorage.getItem("user") as string).username;
  }
}

export default UserDetails;