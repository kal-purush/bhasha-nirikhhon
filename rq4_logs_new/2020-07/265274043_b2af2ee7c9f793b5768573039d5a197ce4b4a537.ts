import ChildProcess from 'child_process';
import fs from 'fs';

export class LauncherSocket {

  process: ChildProcess.ChildProcess;
  bufferStdout: string;

  callbacks: { close: Array<any>, recieve: Array<any> };  // close, recieve

  constructor() {
    this.process = null;
    this.bufferStdout = '';
    this.callbacks = {
      close: [],
      recieve: []
    };
  }

  private handleClose(code, signal): void {
    for (let c of this.callbacks.close)
      c(code, signal);
  }

  private handleRecieve(str: string): void {
    const li = str.split("\n");
    if (li.length === 1) {
      // no breaks
      this.bufferStdout += li[0];
    }
    else {
      const head = li.shift();
      this.handleRecieveLine(this.bufferStdout + head);
      const tail = li.pop();
      this.bufferStdout = tail;
      for (let line of li) {
        this.handleRecieveLine(line);
      }
    }
  }

  private handleRecieveLine(line: string): void {
    let j = null;
    try {
      j = JSON.parse(line);
    }
    catch (e) {
      // TODO: rescue exception
      console.error("LauncherSocket.handleRecieveLine: failed parse json", e);
      return;
    }
    for (let c of this.callbacks['recieve'])
      c(j);
  }

  start(): void {
    const p = ChildProcess.spawn('ruby', ['launcher/launcher.rb'], { stdio: ['pipe', 'pipe', 'inherit'] });
    this.process = p;

    p.on('close', (code, signal) => {
      this.handleClose(code, signal);
    });
    p.stdout.on('data', (data) => {
      // buffstdout.write(data.toString());
      this.handleRecieve(data.toString());
    });
  }

  stop(): void {
    if (this.process !== null)
      this.process.kill();
  }

  send(data: Object): boolean {
    if (this.process === null || this.process.killed)
      return false;
    const j = JSON.stringify(data);
    this.process.stdin.write(j + "\n", () => { /* flushed */ });
    return true;
  }

  on(keyword: string, callback: (args) => void): void {
    if (!this.callbacks[keyword])
      return;
    this.callbacks[keyword].push(callback);
  }
}