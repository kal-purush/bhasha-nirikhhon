
import { create } from 'zustand';
import {WebHookUrlProps } from '../interface';

export const useWebHookURL = create<WebHookUrlProps>((set, get) => ({
    WebHookUrl: {
    IDM: '',
    PASM: '',
    ALERTSHUB: '',
    'MESSAGE-CATALOG': '',
  },
}));
