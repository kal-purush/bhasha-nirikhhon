var EscrowFactory = artifacts.require("EscrowFactory");

module.exports = function (deployer) {
  // Use deployer to state migration tasks.
  deployer.deploy(EscrowFactory);
};