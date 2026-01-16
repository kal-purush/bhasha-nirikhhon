const Mocha = require('mocha');

const mocha = new Mocha({
  reporter: 'mochawesome',
  reporterOptions: {
    reportDir: './docs/mochawesome-apitest',
    quiet: true
  }
});
mocha.addFile('./tests/index.spec.js');
mocha.run(function() {
  console.log('测试完成！');
  process.exit();
});