import CallbackManager from "../lib/launcher/CallbackManager";

type ExecHandlerState = {
  repost: { [jid: string]: (data: any) => void };
}

export class ExecHandler {

  socketStorage: ExecHandlerState;
  socket_id: string;
  launcherCallbackManager: CallbackManager;

  constructor(socket_id: string, launcherCallbackManager: CallbackManager) {
    this.socket_id = socket_id;
    this.socketStorage = { repost: {} };
    this.launcherCallbackManager = launcherCallbackManager;
  }

  handle(data: any, jid: any, resultEmitter: (data: any) => void) {
    const jidStr = JSON.stringify(jid);
    if (data.action == 'run') {  // TODO: 実装が汚すぎる

      (async () => {
        // TODO: 実装が汚すぎる
        const res_data1 = await this.launcherCallbackManager.postp(
          { method: 'store', files: [{ path: 'var/code.rb', data: data.code }] });
        if (!res_data1.success) {
          console.error('launcher failed: method=store: ', res_data1.error);
          resultEmitter(res_data1);
          return;
        }
        const caller = this.launcherCallbackManager.multipost(
          (res_data2) => {
            // note: may call this callback twice or more
            if (res_data2.result && res_data2.result.exited) {
              // finish!
              delete this.socketStorage.repost[jidStr];
              if (!res_data2.success) {
                console.error('launcher failed: method=exec: ', res_data2.error);
                resultEmitter(res_data2);
                return;
              }
            }
            const sid = res_data2.id.sid;
            if (sid !== this.socket_id) return;  // may not happen
            res_data2.id = res_data2.id.jid;
            resultEmitter(res_data2);
          });

        caller.call(null,
          { method: 'exec', cmd: 'ruby', args: ['var/code.rb'], stdin: data.stdin, id: { jid, sid: this.socket_id } }
        );
        this.socketStorage.repost[jidStr] = caller;
      })();

    }
    else if (data.action == 'kill') {  // TODO: 実装が汚すぎる

      const caller = this.socketStorage.repost[jidStr];
      if (!caller) return;  // do nothing if jid is unknown
      caller.call(null,
        { method: 'kill', id: { jid, sid: this.socket_id } }
      );

    }
  }
}