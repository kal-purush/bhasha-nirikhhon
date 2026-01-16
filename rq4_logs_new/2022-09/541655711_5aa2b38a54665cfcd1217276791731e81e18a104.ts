import { ethers } from "ethers";

export interface ContractContextInterface {
  contractProvider: ethers.Contract;
  contractSigner: ethers.Contract;
}