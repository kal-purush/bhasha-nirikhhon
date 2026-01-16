export interface OpenWithItem {
  type?: string;    // mime type
  uri?: string;     // uri to the file, probably NOT a web uri
  name?: string;    // suggested name of the image, iOS 11+ only
  text?: string;    // text to share alongside the item, iOS only
  path?: string;    // path on the device, generally undefined
  utis?: string[];
  base64?: string;
  filepath?: string;
}

export interface OpenWithIntent {
  items: OpenWithItem[]; // shared items
  action?: string;        // type of action requested by the user
  exit?: boolean;         // if true, you should exit the app after processing
}

export interface OpenWith {
  init: (successCallback: () => void, errorCallback: (error: Error) => void) => void;
  addHandler: (handler: (intent: OpenWithIntent) => void) => void;
  load: (item: OpenWithItem, successCallback: (data: string, item: OpenWithItem) => void) => void;
  exit: () => void;
}

interface Cordova {
  openwith: OpenWith;
}

declare global {
  interface Window {
    cordova: Cordova;
  }
}