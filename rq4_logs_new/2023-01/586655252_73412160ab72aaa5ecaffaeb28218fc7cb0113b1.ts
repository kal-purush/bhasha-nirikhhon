import { RequestHandler } from 'express';
import UsersService from '../services/users.service';
import schema from '../validation/rooms.validation';

export default class UsersController {
  usersService: UsersService;

  constructor() {
    this.usersService = new UsersService();
  }
}