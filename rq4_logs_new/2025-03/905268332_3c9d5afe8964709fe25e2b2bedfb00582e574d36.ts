import { NextFunction, Request, Response } from 'express';
import IUserData from '../Interfaces/IUserData';
import UserDataService from '../Services/UserDataService';

export default class UserDataController {
  private res: Response;
  private req: Request;
  private next: NextFunction;
  private userDataService: UserDataService;

  constructor(req: Request, res: Response, next: NextFunction) {
    this.res = res;
    this.req = req;
    this.next = next;
    this.userDataService = new UserDataService();
  }

  public async create() {
    try {
      const { userId } = this.req.body;
      const user: IUserData = { ...this.req.body };
      if (!userId)
        return this.res.status(400).json({ message: 'User ID is required' });
      const newUser = await this.userDataService.create(userId, user);
      if (!newUser)
        return this.res.status(400).json({ message: 'User already exists' });
      return this.res.status(201).json({ message: 'User created: ', newUser });
    } catch (error) {
      return this.res
        .status(500)
        .json({ message: 'Error creating user', error });
    }
  }

  public async getAllUsersData() {
    try {
      const users = await this.userDataService.getAllUsers();
      return this.res.status(200).json({ users: users });
    } catch (error) {
      return this.res
        .status(404)
        .json({ message: 'Error to find users', error });
    }
  }

  public async getUserDataById() {
    try {
      const userId = this.req.params.id;
      const user = await this.userDataService.getUserById(userId);
      if (!user)
        return this.res.status(404).json({ message: 'User not found' });
      return this.res.status(200).json(user);
    } catch (error) {
      return this.res
        .status(404)
        .json({ message: 'Error to find user', error });
    }
  }

  public async updateUserDataById() {
    try {
      const userId = this.req.params.id;
      const user: IUserData = { ...this.req.body };
      const updatedUser = await this.userDataService.updateUserById(
        userId,
        user,
      );
      return this.res
        .status(200)
        .json({ message: 'User updated: ', updatedUser });
    } catch (error) {
      return this.res
        .status(500)
        .json({ message: 'Error to update user', error });
    }
  }

  public async deleteUserDataById() {
    try {
      const userId = this.req.params.id;
      const user = await this.userDataService.getUserById(userId);
      if (!user) {
        return this.res.status(400).json({ message: 'User already deleted' });
      }
      await this.userDataService.deleteUserById(userId);
      return this.res
        .status(200)
        .json({ message: 'User deleted', userDeleted: user });
    } catch (error) {
      return this.res
        .status(500)
        .json({ message: 'Error to delete user', error });
    }
  }
}