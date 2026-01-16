import childProcess from 'child_process';
import getInfo from './getInfo';
import waitOnFn from 'wait-on';

export default class Server {
  server!: childProcess.ChildProcess;

  async serve() {
    const {serveCmd, waitOn} = await getInfo();
    const [cmd, ...args] = serveCmd.split(' ');
    this.server = childProcess.spawn(cmd, args, {
      shell: true,
      stdio: 'pipe'
    });
    this.server.stdout?.on('data', data => console.log(`${data.toString()}`));
    this.server.stderr?.on('data', data => {
      if (data.includes(`ENOENT`)) {
        throw Error(data);
      }
      console.log(data.toString());
    });

    await waitOnFn({
      resources: [waitOn],
      delay: 10000, // initial delay in ms, default 0
      interval: 100, // poll interval in ms, default 250ms
      timeout: 300000, // timeout in ms, default Infinity
      tcpTimeout: 1000, // tcp timeout in ms, default 300ms
      validateStatus: status => {
        console.log(`status: ${status}`);
        return /^20.|^401/.test(`${status}`);
      }
    });
  }

  stop() {
    console.log('stop the server.');
    this.server.kill();
  }
}