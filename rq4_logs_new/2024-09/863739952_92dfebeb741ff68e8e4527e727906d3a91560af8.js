const Calculate = artifacts.require("Calculate");

module.exports = function(deployer) {
  deployer.deploy(Calculate);
};