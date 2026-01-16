import { scan, query, put } from '@services/dynamo-connect';
import { environment } from '@config/environment';
import * as metro from '@util/metrica';
import uuid from 'uuid';
import UserType, { User, AddUserInterface } from '../models/user.type';

export const getUserById = async (userId: string): Promise<UserType> => {
  const mid = metro.metricStart('getUserById');
  try {
    const params: any = {
      TableName: environment.TABLE_NAMES.Users,
      Limit: 1,
      ReturnConsumedCapacity: 'TOTAL'
    };
    const { Items, ...rest } = await query({
      ...params,
      KeyConditionExpression: 'id = :id',
      ExpressionAttributeValues: { ':id': userId }
    });
    metro.metricStop(mid);
    return Items[0];
  } catch (err) {
    metro.metricStop(mid);
    throw err;
  }
};
export const addToUserProjects = async (userId: string, projectId: string) => {
  const mid = metro.metricStart('addToUserProjects');
  try {
    const userData = await getUserById(userId);
    const user = new User({
      ...userData
    });
    user.addToProject(projectId);
    const data = await put({
      TableName: environment.TABLE_NAMES.Users,
      ReturnConsumedCapacity: 'TOTAL',
      Item: user.serialize()
    });
    metro.metricStop(mid);
    return data;
  } catch (err) {
    metro.metricStop(mid);
    throw err.message;
  }
};
export const changeUserFavorites = async (userId: string, mode: 'add' | 'remove', projectId: string) => {
  const mid = metro.metricStart('changeUserFavorites');
  try {
    const userData = await getUserById(userId);
    const user = new User({
      ...userData
    });
    user.changeFavorites(mode, projectId);
    const data = await put({
      TableName: environment.TABLE_NAMES.Users,
      ReturnConsumedCapacity: 'TOTAL',
      Item: user.serialize()
    });
    metro.metricStop(mid);
    return data;
  } catch (err) {
    metro.metricStop(mid);
    throw err.message;
  }
};
export const addUser = async (d: AddUserInterface): Promise<UserType> => {
  const mid = metro.metricStart('addUser');
  try {
    const userId = uuid();
    const now = new Date();
    const user = new User({
      ...d,
      id: userId,
      createdAt: now,
      updatedAt: now
    });
    const data = await put({
      TableName: environment.TABLE_NAMES.Users,
      ReturnConsumedCapacity: 'TOTAL',
      Item: user.serialize()
    });
    const userD = await getUserById(userId);
    metro.metricStop(mid);
    return userD;
  } catch (err) {
    metro.metricStop(mid);
    throw err.message;
  }
};
export const getUserByEmail = async (userEmail: string): Promise<UserType> => {
  const mid = metro.metricStart('getUserByEmail');
  try {
    const params: any = {
      TableName: environment.TABLE_NAMES.Users,
      Limit: 1,
      ReturnConsumedCapacity: 'TOTAL'
    };
    const { Items, ...rest } = await query({
      ...params,
      IndexName: 'UserEmailIndex',
      KeyConditionExpression: 'email = :email',
      ExpressionAttributeValues: { ':email': userEmail }
    });
    if (!Items[0]) {
      return;
    }
    const user = await getUserById(Items[0].id);
    metro.metricStop(mid);
    return user;
  } catch (err) {
    metro.metricStop(mid);
    throw err;
  }
};