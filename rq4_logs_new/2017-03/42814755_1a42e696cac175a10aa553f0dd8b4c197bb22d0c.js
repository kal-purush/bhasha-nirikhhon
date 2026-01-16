// Test script that creates a mesh network then tears it down.

const Mesh = require('./src/mesh');

const main = new Mesh({
  id: "MAIN",
  host: '0.0.0.0',
  port: 1000
})

const nodes = [1,2,3,4,5,6,7,8,9,10]
  .map((i) => new Mesh({
    host: '127.0.0.1',
    port: 1000 + i,
    connect: { host: '127.0.0.1', port: 1000}
  }))

setTimeout(() => {
  nodes.forEach((x) => x.close());
  main.close();
}, 5000);