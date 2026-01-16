import { MMKV, Mode } from 'react-native-mmkv';

export const storage = new MMKV({
  id: 'user-storage',
  mode: Mode.MULTI_PROCESS,
});

export const getNotificationsEnabled = () => storage.getBoolean('notificationsEnabled') ?? false;
export const setNotificationsEnabled = (value: boolean) => storage.set('notificationsEnabled', value);