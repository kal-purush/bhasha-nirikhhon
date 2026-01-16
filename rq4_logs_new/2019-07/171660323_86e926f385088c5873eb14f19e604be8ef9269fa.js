const envDirAdd = ['alpha', 'beta'];
const envDirNow = process.argv[2] || '';

if (envDirNow) {
    console.log(envDirNow);
}