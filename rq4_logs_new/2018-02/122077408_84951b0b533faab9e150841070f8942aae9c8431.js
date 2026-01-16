const { spawn } = require('child_process');

module.exports = (process, args, stdin) => new Promise((resolve, reject) => {
    const child = spawn(process, args || []);
    child.stdin.setEncoding('utf-8');
    child.stdin.write(stdin + "\n");
    child.stdin.end()
    

    child.on('error', err  =>
        reject(err)
    )
    
    var result = ''
    child.stdout.on('data', data =>
        result += data
    )
    
    child.on('close', code => 
        code === 0
        ? resolve(result)
        : reject(code)
    )
})