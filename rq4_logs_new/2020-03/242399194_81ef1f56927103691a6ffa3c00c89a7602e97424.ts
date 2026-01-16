import { spawn } from 'child_process';

export default () =>
  new Promise((resolve, reject) => {
    const mockApi = spawn('amplify', ['mock', 'api']);
    let err = '';

    mockApi.stderr.on('data', (data) => {
      const str = data.toString('utf8');
      if (str.toLowerCase().includes('error')) {
        err += str;
      }
    });
    setTimeout(() => {
      if (err) {
        try {
          mockApi.kill();
        } catch {
          /* me no care */
        }
        reject(err);
      } else {
        process.env.AMPLIFY_PID = String(mockApi.pid);
        resolve();
      }
    }, 5000);
  });