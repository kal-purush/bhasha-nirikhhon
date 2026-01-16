import {Prompter} from './prompter';
import {Context} from '../context';

export class WindowOpener {

  private context: Context;

  constructor(context: Context) {
    this.context = context;
  }

  public openRakr(path: string, parameters?: { [key: string]: any; }): Promise<Window> {
    let url = this.context.resolveFullPath(path);
    return this.open(url, parameters);
  }

  public open(url: string, parameters?: { [key: string]: any; }): Promise<Window> {
    return new Promise((resolve, reject) => {
      if (parameters) {
        if (url.indexOf('?') < 0) {
          url += '?';
        }
        let normalizedParamters = [];
        for (let key in parameters) {
          normalizedParamters.push(encodeURIComponent(key) + '=' + encodeURIComponent(parameters[key]));
        }
        url += normalizedParamters.join('&');
      }

      let newWindow = window.open(url);
      if (newWindow) {
        resolve(newWindow);

      } else {
        // TODO Created a trust event to bypass blocking
        // trust event: https://www.w3.org/TR/DOM-Level-3-Events/#trusted-events
        Prompter.prompt('請允許開啟彈跳式視窗。');
        reject('Browser blocked window popup.');
      }
    })
  }
}