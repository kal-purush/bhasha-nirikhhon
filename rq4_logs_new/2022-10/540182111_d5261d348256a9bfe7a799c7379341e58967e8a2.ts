import { TransactionRecordQuery, TokenInfoQuery, AccountBalanceQuery, TransactionId, Client, AccountId, TokenId, AccountBalance, ContractId } from "@hashgraph/sdk";

export const transactionRecordQuery = async (txId: string | TransactionId, client: Client) => {
	const transactionRecordQuery = await new TransactionRecordQuery()
		.setTransactionId(txId)
		.setIncludeChildren(true)
		.execute(client);
	return transactionRecordQuery;
}

export const tokenInfoQuery = async (tkId: string | TokenId, client: Client) => {
	const info = await new TokenInfoQuery().setTokenId(tkId).execute(client);
	return info;
}

export const checkAccountBalance = async (accountId: string | AccountId, tokenId: string | TokenId, client: Client) => {
	const balanceCheckTx: AccountBalance = await new AccountBalanceQuery().setAccountId(accountId).execute(client);
	
	if (balanceCheckTx.tokens === null) { throw new Error('tokens is nul')}

	console.log(
		`- Balance of account ${accountId}: ${balanceCheckTx.hbars.toString()} + ${balanceCheckTx.tokens._map.get(
			tokenId.toString()
		)} unit(s) of token ${tokenId}`
	);
}

export const checkContractBalance = async (contractId: string | ContractId, tokenId: string | TokenId, client: Client) => {
	const balanceCheckTx: AccountBalance = await new AccountBalanceQuery().setContractId(contractId).execute(client);
	
	if (balanceCheckTx.tokens === null) { throw new Error('tokens is nul')}

	console.log(
		`- Balance of account ${contractId}: ${balanceCheckTx.hbars.toString()} + ${balanceCheckTx.tokens._map.get(
			tokenId.toString()
		)} unit(s) of token ${tokenId}`
	);
}