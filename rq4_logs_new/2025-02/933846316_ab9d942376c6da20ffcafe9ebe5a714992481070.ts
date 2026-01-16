// make __TAURI__ window type safe
declare global {
  interface Window {
    __TAURI__?: {
      fs: {
        readTextFile: (path: string) => Promise<string>
      }
    }
  }
}

export {}