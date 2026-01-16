import mongoose from 'mongoose';
import IUserData from '../Interfaces/IUserData';

export default class UserDataDomain implements IUserData {
  public userId: mongoose.Types.ObjectId;
  public username: string;
  public age: number;
  public gender: 'Feminino' | 'Masculino';
  public height: number;
  public weight: number;
  public level:
    | 'Sedentario'
    | 'Levemente ativo'
    | 'Moderadamente ativo'
    | 'Muito ativo';
  public objective: 'Perder peso' | 'Ganhar musculo' | 'Manutencao';
  public trainingFrequency: number;

  constructor(user: IUserData) {
    this.userId = user.userId;
    this.username = user.username;
    this.age = user.age;
    this.gender = user.gender;
    this.height = user.height;
    this.weight = user.weight;
    this.level = user.level;
    this.objective = user.objective;
    this.trainingFrequency = user.trainingFrequency;
  }
}