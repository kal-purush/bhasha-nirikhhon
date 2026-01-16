import AsyncStorage from "@react-native-async-storage/async-storage"

const namespace = "FSO22_REPO_RATER_STORAGE"
const key = `${namespace}:AUTH`

interface AuthStorage {
  getAccessToken: () => Promise<string | null>
  setAccessToken: (token: string) => Promise<void>
  removeAccessToken: () => Promise<void>
}

const storage: AuthStorage = {
  getAccessToken: async () => {
    return await AsyncStorage.getItem(key)
  },
  setAccessToken: async (token) => {
    await AsyncStorage.setItem(key, token)
  },
  removeAccessToken: async () => {
    await AsyncStorage.removeItem(key)
  }
}

export default storage