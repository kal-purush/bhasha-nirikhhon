import {HardhatRuntimeEnvironment} from 'hardhat/types';
import {DeployFunction} from 'hardhat-deploy/types';

const func: DeployFunction = async function (hre: HardhatRuntimeEnvironment) {
    const {deployments, getNamedAccounts} = hre;
    const {deploy, execute} = deployments;

    const {deployer} = await getNamedAccounts();
    const owner = "0x3946d96a4b46657ca95CBE85d8a60b822186Ad1f";

    // // DAOStaking
    await deploy('DAOStaking', {
        from: deployer,
        args: [owner],
        log: true,
    });
    const DAOStaking = await deployments.get('DAOStaking');

    // DAOFactory
    await deploy('DAOFactory', {
        from: deployer,
        args: [owner, DAOStaking.address],
        log: true,
    });

};
export default func;
func.tags = ['DAOStaking', 'DAOFactory'];