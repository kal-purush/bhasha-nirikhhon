import { Client, Wallet, Obi, Fee, Coin, Message, Transaction } from '@bandprotocol/bandchain.js'

const { PrivateKey } = Wallet
const grpcUrl = "https://laozi-testnet4.bandchain.org/grpc-web"
const client = new Client(grpcUrl)

const mnemonic = 's'

const privKey = PrivateKey.fromMnemonic(mnemonic)
const pubKey = privKey.toPubkey()
const sender = pubKey.toAddress().toAccBech32();

interface CryptoPriceProps {
  symbols: string[] | string
  multiplier: number
  askCount: number
  minCount: number
  clientId: string
  feeLimit: number
  prepareGas: number
  executeGas: number
}

export const requestCryptoPrice = async ({
  symbols,
  multiplier,
  askCount,
  minCount,
  clientId,
  feeLimit,
  prepareGas,
  executeGas
}: CryptoPriceProps,
  setLoading: React.Dispatch<React.SetStateAction<boolean>>) => {
  setLoading(true)
  const obi = new Obi("{symbols:[string],multiplier:u64}/{rates:[u64]}")
  const encodedCallData = obi.encodeInput({ symbols, multiplier })
  const oracleScriptId = 37

  let coinFeeLimit = new Coin()
  coinFeeLimit.setDenom("uband")
  coinFeeLimit.setAmount(String(feeLimit))

  const requestMessage = new Message.MsgRequestData(
    oracleScriptId,
    encodedCallData,
    askCount,
    minCount,
    clientId,
    sender,
    [coinFeeLimit],
    prepareGas,
    executeGas
  )

  let feeCoin = new Coin()
  feeCoin.setDenom("uband")
  feeCoin.setAmount("50000")

  const fee = new Fee()
  fee.setAmountList([feeCoin])
  fee.setGasLimit(1000000)

  const chainId = await client.getChainId()
  const txn = new Transaction()
  txn.withMessages(requestMessage)
  await txn.withSender(client, sender)
  txn.withChainId(chainId)
  txn.withFee(fee)
  txn.withMemo("")

  const signDoc = txn.getSignDoc(pubKey)
  const signature = privKey.sign(signDoc)

  const txRawBytes = txn.getTxData(signature, pubKey)

  const sendTx = await client.sendTxBlockMode(txRawBytes)
  setLoading(false)
  return sendTx
}