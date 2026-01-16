import { Schema, Model, model, models } from 'mongoose';
import IUserData from '../Interfaces/IUserData';

export default class UserDataODM {
  private static model: Model<IUserData>;

  constructor() {
    if (!UserDataODM.model) {
      const schema = new Schema<IUserData>({
        userId: {
          type: Schema.Types.ObjectId,
          ref: 'User',
          required: true,
        },
        age: { type: Number, required: true },
        gender: { type: String, required: true },
        height: { type: Number, required: true },
        weight: { type: Number, required: true },
        level: { type: String, required: true },
        objective: { type: String, required: true },
        trainingFrequency: { type: Number, required: true },
      });
      UserDataODM.model =
        models.UserData || model<IUserData>('UserData', schema);
    }
  }

  public async create(userData: IUserData) {
    return UserDataODM.model.create({ ...userData });
  }

  public async getAllUsers() {
    const users = await UserDataODM.model.find();
    return users;
  }

  public async getUserById(id: string) {
    const user = await UserDataODM.model.findOne({ userId: id });
    return user;
  }

  public async getUserByUsername(username: string) {
    const user = await UserDataODM.model.findOne({ username });
    return user;
  }

  public updateUserById(id: string, user: IUserData) {
    return UserDataODM.model.findByIdAndUpdate({ userId: id }, user, {
      new: true,
    });
  }

  public async deleteUserById(id: string) {
    return UserDataODM.model.deleteOne({ userId: id });
  }
}