import getEnv from '../envs';
import { getContract } from '../poap-helper';
import { TokenOwner } from '../types';


async function getTokenOwner(tokenId: string | number): Promise<TokenOwner> {
    const env = getEnv();
    const contract = getContract(env.poapAdmin);
    const eventId = await contract.functions.tokenEvent(tokenId);
    const owner = await contract.functions.ownerOf(tokenId);
  
    return {
      eventId: eventId.toNumber(),
      tokenId: tokenId.toString(),
      owner,
    };
}

async function getTokenOwners() {
    const lastTokenId = 4901;
    let responseArray = []

    const arrayTimes = Array.from({ length: lastTokenId });
    for await (const i of arrayTimes) {
        const TokenOwner = await getTokenOwner(i.toNumber());
        if (TokenOwner) {
            responseArray.push(TokenOwner);
        }
    }

    console.log(responseArray);
}

getTokenOwners();