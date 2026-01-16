import { LocalStorageRepository } from "../../domain/repositories";
import AsyncStorage from "../../ui/node_modules/@react-native-async-storage/async-storage";

export class LocalStorageImp implements LocalStorageRepository {
  async getItem<T>(key: string): Promise<T | undefined> {
    const data = await AsyncStorage.getItem(key);
    if (!data) return;
    const dataObj = JSON.parse(data);
    return dataObj;
  }

  async setItem(key: string, data: any): Promise<void> {
    if (typeof data === "string") {
      await AsyncStorage.setItem(key, data);
      return;
    }
    const dataStr = JSON.stringify(data);
    await AsyncStorage.setItem(key, dataStr);
  }

  async removeItem(key: string): Promise<void> {
    await AsyncStorage.removeItem(key);
  }
}