const bitcoin = require("bitcoinjs-lib");
const axios = require("axios");

async function getUTXO(address) {
  const result = await axios.get(
    "https://api.blockcypher.com/v1/btc/test3/addrs/" + address + "/full"
  );
  const txs = result.data.txs;

  let utxo = [];

  // el siguiente for aninado podria mejorarse usando: map, reduce & filter
  for (let i = 0; i < txs.length; i++) {
    const tx = txs[i];
    const outs = tx.outputs;

    for (let j = 0; j < outs.length; j++) {
      const out = outs[j];
      if (!out.spent_by) {
        if (out.addresses.indexOf(address) !== -1) {
          utxo.push({
            txId: tx.hash,
            index: j,
            value: out.value,
          });
        }
      }
    }
  }

  return utxo;
}

async function sendTx() {
  const satoshisToSend = 1000;
  const fee = 1000;
  let totalAmount = 0;
  let keyPair = bitcoin.ECPair.fromWIF(
    "cVN8HhweU4obeakVQyRU1nJaKRvMPVPWAArGijGchc3DTkdsk5FK",
    bitcoin.networks.testnet
  );

  const utxos = await getUTXO("mtdHT5LSbvw449zDGzvPD2eWrXPNmpfYge");

  let tx = new bitcoin.TransactionBuilder(bitcoin.networks.testnet);
  utxos.forEach((utxo) => {
    tx.addInput(utxo.txId, utxo.index);
    totalAmount += utxo.value;
  });
  tx.addOutput("mqQHwpYeRnZdbZmzGaVDGiyFQmmafDMkmW", satoshisToSend);
  tx.addOutput(
    "mtdHT5LSbvw449zDGzvPD2eWrXPNmpfYge",
    totalAmount - satoshisToSend - fee
  );

  for (let i = 0; i < utxos.length; i++) {
    tx.sign(i, keyPair);
  }

  const body = tx.build().toHex();
  // console.log(body);

  await axios.post("https://api.blockcypher.com/v1/btc/test3/txs/push", {
    tx: body,
  });
}