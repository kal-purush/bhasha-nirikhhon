exports.deploy = (domain) => {
  process.argv.push(domain);
  return require('./../tasks/deploy').default;
}