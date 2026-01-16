import CallbackManager from "../../lib/launcher/CallbackManager";
import { asyncError, ResultEmitter, Runnable } from "./TaskUtil";

export class TaskCpp {

  private socketId: string;
  private launcherCallbackManager: CallbackManager;
  private resultEmitter: ResultEmitter;
  private finalize: Runnable;
  private handleKill?: Runnable;

  constructor(socketId: string, launcherCallbackManager: CallbackManager, resultEmitter: ResultEmitter, finalize: Runnable) {
    this.socketId = socketId;
    this.launcherCallbackManager = launcherCallbackManager;
    this.resultEmitter = resultEmitter;
    this.finalize = finalize;
    this.handleKill = null;
    this.kill = this.kill.bind(this);
  }

  kill(): void {
    this.handleKill?.call(this);
  }

  private async phase1(data: any, jid: any) {
    const res_data = await this.launcherCallbackManager.postp(
      { method: 'store', files: [{ path: 'var/code.cpp', data: data.code }] });
    res_data.id = res_data.id.jid;
    if (!res_data.success) {
      this.resultEmitter(res_data);
      return await asyncError('launcher failed: method=store: ' + res_data.error);
    }
  }

  private async phase2(data: any, jid: any) {
    return new Promise((resolve, reject) => {
      this.launcherCallbackManager.post(
        { method: 'exec', cmd: 'g++', args: ['-std=c++17', '-O3', '-Wall', '-o', 'var/code', 'var/code.cpp'], stdin: data.stdin, id: { jid, sid: this.socketId } },
        (res_data) => {
          // note: call this callback twice or more
          res_data.id = res_data.id.jid;
          console.log('phase2 process: ', res_data);
          if (!res_data.success) {
            this.resultEmitter(res_data);
            reject(asyncError('launcher failed: method=store: ' + res_data.error));
            return;
          }
          if (res_data.result.exited) {
            if (res_data.result.exitstatus === 0) {
              res_data.continue = true;
              this.resultEmitter(res_data);
              resolve();
              return;
            }
            else {
              this.resultEmitter(res_data);
              reject(asyncError('compile error'));
              return;
            }
          }
        });
    });

  }

  private phase3(data: any, jid: any) {
    return new Promise((resolve, reject) => {
      const caller = this.launcherCallbackManager.multipost(
        (res_data2) => {
          // note: call this callback twice or more
          res_data2.id = res_data2.id.jid;
          if (res_data2.result && res_data2.result.exited) {
            this.finalize();
            this.handleKill = null;
            if (res_data2.success) {
              resolve();
            } else {
              // note: NOT runtime error (it means rejected a bad query)
              console.error('launcher failed: method=exec: ', res_data2.error);
              reject();
            }
          }
          this.resultEmitter(res_data2);
        });
      caller.call(null,
        { method: 'exec', cmd: 'var/code', args: [], stdin: data.stdin, id: { jid, sid: this.socketId } }
      );
      this.handleKill = () => {
        caller.call(null,
          { method: 'kill', id: { jid, sid: this.socketId } }
        );
      };
    });
  }

  async startAsync(data: any, jid: any) {
    console.log('task start:' + jid);
    try {
      await this.phase1(data, jid);
      await this.phase2(data, jid);
      await this.phase3(data, jid);
    } catch (e) {
      console.error(e);  // include compile error...
      this.finalize();
    }
    console.log('task complete:' + jid);
  }
}