import UserDataODM from '../Models/UserData';
import IUserData from '../Interfaces/IUserData';
import UserDataDomain from '../Domains/UserDataDomain';

export default class UserDataService {
  private userDataODM = new UserDataODM();

  private createUserData(userData: IUserData) {
    return new UserDataDomain(userData);
  }

  public async create(userId: string, userData: IUserData) {
    const existingUserData = await this.userDataODM.getUserById(userId);
    if (existingUserData) return null;
    const newUser = await this.userDataODM.create(userData);
    return this.createUserData(newUser);
  }

  public async getAllUsers() {
    const users = await this.userDataODM.getAllUsers();
    return users.map((userData) => this.createUserData(userData));
  }

  public async getUserById(userId: string) {
    const userData = await this.userDataODM.getUserById(userId);
    if (!userData) return null;
    return this.createUserData(userData);
  }

  public async updateUserById(userId: string, userData: IUserData) {
    const existingUser = await this.userDataODM.getUserById(userId);
    if (!existingUser) return false;
    const updatedUser = await this.userDataODM.updateUserById(userId, userData);
    return updatedUser;
  }

  public async deleteUserById(userId: string) {
    return this.userDataODM.deleteUserById(userId);
  }
}