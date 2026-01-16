import CallbackManager from "../../lib/launcher/CallbackManager";
import { TaskRuby } from "./TaskRuby";
import { ResultEmitter, Runnable } from "./TaskUtil";


export class TaskFactory {
  private socketId: string;
  private launcherCallbackManager: CallbackManager;
  private resultEmitter: ResultEmitter;
  private finalize: Runnable;
  private handleKill: Runnable;

  constructor(socketId: string, launcherCallbackManager: CallbackManager, resultEmitter: ResultEmitter, finalize: Runnable) {
    this.socketId = socketId;
    this.launcherCallbackManager = launcherCallbackManager;
    this.resultEmitter = resultEmitter;
    this.finalize = finalize;

    this.handleKill = null;
  }

  generate(lang: string): any {
    if (lang === 'ruby')
      return new TaskRuby(this.socketId, this.launcherCallbackManager, this.resultEmitter, this.finalize);
    return null;
  }
}