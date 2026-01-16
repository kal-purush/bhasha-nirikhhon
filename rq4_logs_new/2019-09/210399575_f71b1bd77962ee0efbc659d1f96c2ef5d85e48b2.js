module.exports = function ls() {
    process.stdout.write('prompt > ');
    const ls = fs.readdir('./', 'utf8', (err, files) => {
        if (err) {
            throw err
        } else {
        process.stdout.write(files.join('\n'))
        process.stdout.write('\nprompt > ');
        }
    });
}