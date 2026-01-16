import AsyncStorage from '@react-native-community/async-storage';

export async function getObjectStorage(storageKey) {
    let data = await AsyncStorage.getItem(storageKey) || '{}';

    return JSON.parse(data);
}

export async function setObjectStorage(storageKey, key, value) {
    let data = await AsyncStorage.getItem(storageKey) || '{}';

    data = JSON.parse(data);
    data[key] = value;

    await AsyncStorage.setItem(storageKey, JSON.stringify(data));
}