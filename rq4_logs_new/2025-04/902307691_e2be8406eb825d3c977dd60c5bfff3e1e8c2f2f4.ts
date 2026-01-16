
import { NodeSSH } from 'node-ssh'
const ssh = new NodeSSH()

ssh.connect({
  host: '192.168.1.248',
  port: 22,
  username: 'user',
  password: 'password',
})
  .then(async () => {
    await ssh.execCommand('dir')
      .then(function (result) {
        console.log('STDOUT: ' + result.stdout)
        console.log('STDERR: ' + result.stderr)
      })
  })
