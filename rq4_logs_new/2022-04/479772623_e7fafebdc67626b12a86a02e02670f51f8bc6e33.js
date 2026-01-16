const express = require('express');
const SHA256 = require('crypto-js/sha256');
const SHA1 = require('crypto-js/sha1');
const {NodeClient, WalletClient} = require('./node_modules/@oipwg/fclient');
const apiKey = require('dotenv').config().parsed.apiKey;
const {RPCWallet} = require('js-oip/lib/modules/wallets');
const axios = require('axios');
const { Modules } = require('js-oip');
const req = require('express/lib/request');
const { response } = require('express');
const http = require('http');
const https = require('https');
const { on } = require('events');
const oipProto = require('oip-protobufjs');
const buildOipDetails = oipProto.buildOipDetails;
const recordProtoBuilder = oipProto.recordProtoBuilder;
const app = express();
app.use(express.json());

// listen on port 3000
app.listen(3000, () => {
  console.log('OIP API listening on port 3000');
});

const clientOptions = {
  network: 'mainnet',
  port: 7313,
  apiKey: apiKey
}

const walletClientOptions = {
  network: 'mainnet',
  port: 7315,
  apiKey: apiKey
}

const client = new NodeClient(clientOptions);
const walletClient = new WalletClient(walletClientOptions);

// --------------------------------------------------------------------------------
// Functions

// get blockchain info including sync status
async function getinfo() {
  const result = await client.getInfo();
  return result
}

// get info about connected peers
async function getPeerInfo() {
  const result = await client.execute('getpeerinfo');
  return result
}

// create a wallet
async function createWallet(id, options) {
  try{
    const result = await walletClient.createWallet(id, options);
    return result
  } catch (err) {
    console.log('error in createWallet:', err);
    return err
  }
}

// generate a receiving address
async function createAddress(id, account){
  const result = await walletClient.createAddress(id, account);
  // console.log(result);
  return result
}

// unlock a wallet with passprhase
async function walletpassphrase(passphrase, timeout) {
  timeout = 300;
  try{
    const result = await walletClient.walletpassphrase(passphrase, timeout);
    return result
  } catch (err) {
    return err
  }
}

// select a wallet to use
async function selectwallet(id){
  const result = await walletClient.execute('selectwallet', [id]);
  return result
}

// this is probably deprecated, turn it off for now and see if anything breaks
// async function getwalletinfo() {
//   const result = await walletClient.execute('getwalletinfo');
//   return result
// }

// encrypt a wallet
async function encryptwallet(passphrase, id) {
  const wallet = walletClient.wallet(id);
  const result = await walletClient.execute('encryptwallet', [passphrase]);
  // console.log('result of encryptwallet', result);
  return result
}

// get master HD Key from wallet
async function getMasterHDKey(id) {
  const wallet = walletClient.wallet(id);
  const result = await wallet.getMaster();
  return result
}

// check whether this should just be renamed to getFpub and deprecate the other one
async function getwalletaccount(id, account) {
  const wallet = walletClient.wallet(id);
  const result = await wallet.getAccount(account);
  const fpub = result.accountKey
  return fpub
}

// get fpub
function getFpub(id, account) {
  return new Promise(function(resolve, reject) {
    getwalletaccount(id, account).then(result => {
      let fpub = result;
      resolve(fpub);
    })
  })
}

// get a wallets WIF (wallet import format - private key)
async function getwif(id, address, passphrase) {
  const wallet = walletClient.wallet(id);
  const result = await wallet.getWIF(address, passphrase);
  const wif = result.privateKey;
  return wif
}

// get wallet info, including balance
async function getwalletinfo(id) {
  const wallet = walletClient.wallet(id);
  const result = await wallet.getInfo();
  return result
}

// sign message with wallet private key
async function signMessage(wif, pubKey, message) {
  network = 'mainnet';
  let walletRPC = new RPCWallet({
    publicAddress: pubKey,
    wif,
    network,
    rpc: {
      port: 7313,
      host: '127.0.0.1',
      username: 'x',
      password: apiKey   
    }
  })
  const result = await walletRPC.signMessage(message)
  return result
}

// take formatted data that needs to be published, 
// turn it into oipDetails and then turn it into 
// serialized protobuf hex64 data for publishing
async function getSignedP64FloData(data, wif){
  try {
    const details = buildOipDetails(data.details)
    const { signedMessage64 } = await recordProtoBuilder({
      details,
      wif,
      network: 'mainnet'
    });
    return 'p64:' + signedMessage64;
  } catch (err) {
    return 'Failed at publishRecord: ' + err;
  }
}

// create a transaction, possibly deprecate this
async function createTX(options) {
  const result = await wallet.createTX(options);
  return result
}

// send floData to chain
async function sendFloDataToChain(floData, publicAddress, wif, previousTransactionOutput) {
  network = 'mainnet';
  let walletRPC = new RPCWallet({
    publicAddress: publicAddress,
    wif,
    network,
    rpc: {
      port: 7313,
      host: '127.0.0.1',
      username: 'x',
      password: apiKey   
    }
  })
  const result = await walletRPC.sendDataToChain(floData, previousTransactionOutput);
  return result
}

// prepare and format publisher registration data
async function prepRegistration(pubKey, publisherName, fpub, wif) {
  const publisher = {
    myMainAddress: wif,
    descriptor:
      'Ck4KB3AucHJvdG8SEm9pcFByb3RvLnRlbXBsYXRlcyInCgFQEgwKBG5hbWUYASABKAkSFAoMZmxvQmlwNDRYUHViGAIgASgJYgZwcm90bzM=',
    name: 'tmpl_433C2783',
    payload: {
      name: publisherName,
      floBip44XPub: fpub
    }
  }
  
  let registerPublisherData = [publisher]

  registerPublisherData.pubKey = pubKey;
  
  function createRegistration(registerPublisherData) {
    return {
      details: registerPublisherData,
      myMainAddress: '',
      pubKey
    }
  }

  let data = createRegistration(registerPublisherData)
  return data;
}

// prepare the first transaction of a multipart record
async function prepareFirstTXofMP(id, pubKey, wif, mpx){
  let floDataArr = [];
  let outputsArr = [];
  
  // Signed64 is less than 1040
  if (!Array.isArray(mpx)) {
    console.log('mpx is not array');
    let transactionOutput = await sendFloDataToChain(mpx, pubKey, wif);
    let txid = transactionOutput.txid;
    floDataArr.push(txid);
    outputsArr.push(transactionOutput);
  } else {
    mpx[0].setAddress(pubKey);
    let signatureData = mpx[0].getSignatureData();
    return signatureData;
  }
}

// get floData for the first transaction of a multipart record
async function getFloDataForFirstTXofMP(id, pubKey, wif, mpx, sig) {
  mpx[0].setSignature(sig);

    const floData = `${mpx[0].prefix}(${mpx[0].part},${mpx[0].max},${mpx[0].address},,${mpx[0].signature}):${mpx[0].data}`;
    return floData;
}

// send the rest of the transactions in a multipart record
async function sendRestOfTXsOfMP(id, pubKey, wif, mpx, referenceTransactionOutput) {
  let floDataArr = [];
  let outputsArr = [];
    let referenceTxid = referenceTransactionOutput.txid;
    floDataArr.push(referenceTxid);
    outputsArr.push(referenceTransactionOutput);

    //First post request has come back ok, start the loop post request
    if (referenceTxid) {

      let userRequests=[];
      for (let i = 1; i < mpx.length; i++) {
        mpx[i].setReference(referenceTxid);
        mpx[i].setAddress(pubKey);
        let time = new Date().toISOString()
        let sig = await signMessage(wif, pubKey,mpx[i].getSignatureData());    
        mpx[i].setSignature(sig);
        time = new Date().toISOString()
        let transactionOutput = await sendFloDataToChain(`${mpx[i].prefix}(${mpx[i].part},${mpx[i].max},${mpx[i].address},${mpx[i].reference},${mpx[i].signature}):${mpx[i].data}`, pubKey, wif, outputsArr.slice(-1));
        time = new Date().toISOString()
        floDataArr.push(transactionOutput.txid);
        outputsArr.push(transactionOutput);
        const recordTxidArray = await Promise.all([sig, transactionOutput]).then((values) => {
          return floDataArr
        });
      return recordTxidArray
      }
    }
}

// this might be entirely moot, investigate
async function walletData(pubKey, floData){
  const walletdata = {};
  walletdata.pub = pubKey;
  walletdata.floData = floData;
  return walletdata;
};

// gets info about a specified template
async function getTemplateInfo(templateType){
  switch (templateType) {
    case 'basic': 
      templateName = 'tmpl_20AD45E7';
      descriptor = 'CpwSCgdwLnByb3RvEhJvaXBQcm90by50ZW1wbGF0ZXMi9BEKAVASDAoEbmFtZRgBIAEoCRITCgtkZXNjcmlwdGlvbhgCIAEoCRIMCgRkYXRlGAMgASgEEhoKCGxhbmd1YWdlGAQgASgOMghMYW5ndWFnZRIUCgZhdmF0YXIYBSABKAsyBFR4aWQSDwoHdGFnTGlzdBgGIAMoCRIQCghub3RlTGlzdBgHIAMoCRIVCgd1cmxMaXN0GAggAygLMgRUeGlkGhMKBFR4aWQSCwoDcmF3GAEgASgMIrwQCghMYW5ndWFnZRIWChJMYW5ndWFnZV9VTkRFRklORUQQABIPCgtMYW5ndWFnZV9BRhABEg8KC0xhbmd1YWdlX0FNEAISDwoLTGFuZ3VhZ2VfQVIQAxIQCgxMYW5ndWFnZV9BUk4QBBIPCgtMYW5ndWFnZV9BUxAFEg8KC0xhbmd1YWdlX0FaEAYSDwoLTGFuZ3VhZ2VfQkEQBxIPCgtMYW5ndWFnZV9CRRAIEg8KC0xhbmd1YWdlX0JHEAkSDwoLTGFuZ3VhZ2VfQk4QChIPCgtMYW5ndWFnZV9CTxALEg8KC0xhbmd1YWdlX0JSEAwSDwoLTGFuZ3VhZ2VfQlMQDRIPCgtMYW5ndWFnZV9DQRAOEg8KC0xhbmd1YWdlX0NPEA8SDwoLTGFuZ3VhZ2VfQ1MQEBIPCgtMYW5ndWFnZV9DWRAREg8KC0xhbmd1YWdlX0RBEBISDwoLTGFuZ3VhZ2VfREUQExIQCgxMYW5ndWFnZV9EU0IQFBIPCgtMYW5ndWFnZV9EVhAVEg8KC0xhbmd1YWdlX0VMEBYSDwoLTGFuZ3VhZ2VfRU4QFxIPCgtMYW5ndWFnZV9FUxAYEg8KC0xhbmd1YWdlX0VUEBkSDwoLTGFuZ3VhZ2VfRVUQGhIPCgtMYW5ndWFnZV9GQRAbEg8KC0xhbmd1YWdlX0ZJEBwSEAoMTGFuZ3VhZ2VfRklMEB0SDwoLTGFuZ3VhZ2VfRk8QHhIPCgtMYW5ndWFnZV9GUhAfEg8KC0xhbmd1YWdlX0ZZECASDwoLTGFuZ3VhZ2VfR0EQIRIPCgtMYW5ndWFnZV9HRBAiEg8KC0xhbmd1YWdlX0dMECMSEAoMTGFuZ3VhZ2VfR1NXECQSDwoLTGFuZ3VhZ2VfR1UQJRIPCgtMYW5ndWFnZV9IQRAmEg8KC0xhbmd1YWdlX0hFECcSDwoLTGFuZ3VhZ2VfSEkQKBIPCgtMYW5ndWFnZV9IUhApEhAKDExhbmd1YWdlX0hTQhAqEg8KC0xhbmd1YWdlX0hVECsSDwoLTGFuZ3VhZ2VfSFkQLBIPCgtMYW5ndWFnZV9JRBAtEg8KC0xhbmd1YWdlX0lHEC4SDwoLTGFuZ3VhZ2VfSUkQLxIPCgtMYW5ndWFnZV9JUxAwEg8KC0xhbmd1YWdlX0lUEDESDwoLTGFuZ3VhZ2VfSVUQMhIPCgtMYW5ndWFnZV9KQRAzEg8KC0xhbmd1YWdlX0tBEDQSDwoLTGFuZ3VhZ2VfS0sQNRIPCgtMYW5ndWFnZV9LTBA2Eg8KC0xhbmd1YWdlX0tNEDcSDwoLTGFuZ3VhZ2VfS04QOBIQCgxMYW5ndWFnZV9LT0sQORIPCgtMYW5ndWFnZV9LTxA6Eg8KC0xhbmd1YWdlX0tZEDsSDwoLTGFuZ3VhZ2VfTEIQPBIPCgtMYW5ndWFnZV9MTxA9Eg8KC0xhbmd1YWdlX0xUED4SDwoLTGFuZ3VhZ2VfTFYQPxIPCgtMYW5ndWFnZV9NSRBAEg8KC0xhbmd1YWdlX01LEEESDwoLTGFuZ3VhZ2VfTUwQQhIPCgtMYW5ndWFnZV9NThBDEhAKDExhbmd1YWdlX01PSBBEEg8KC0xhbmd1YWdlX01SEEUSDwoLTGFuZ3VhZ2VfTVMQRhIPCgtMYW5ndWFnZV9NVBBHEg8KC0xhbmd1YWdlX05CEEgSDwoLTGFuZ3VhZ2VfTkUQSRIPCgtMYW5ndWFnZV9OTBBKEg8KC0xhbmd1YWdlX05OEEsSEAoMTGFuZ3VhZ2VfTlNPEEwSDwoLTGFuZ3VhZ2VfT0MQTRIPCgtMYW5ndWFnZV9PUhBOEg8KC0xhbmd1YWdlX1BBEE8SDwoLTGFuZ3VhZ2VfUEwQUBIQCgxMYW5ndWFnZV9QUlMQURIPCgtMYW5ndWFnZV9QVBBSEhAKDExhbmd1YWdlX1FVVBBTEhAKDExhbmd1YWdlX1FVWhBUEg8KC0xhbmd1YWdlX1JNEFUSDwoLTGFuZ3VhZ2VfUk8QVhIPCgtMYW5ndWFnZV9SVRBXEg8KC0xhbmd1YWdlX1JXEFgSEAoMTGFuZ3VhZ2VfU0FIEFkSDwoLTGFuZ3VhZ2VfU0EQWhIPCgtMYW5ndWFnZV9TRRBbEg8KC0xhbmd1YWdlX1NJEFwSDwoLTGFuZ3VhZ2VfU0sQXRIPCgtMYW5ndWFnZV9TTBBeEhAKDExhbmd1YWdlX1NNQRBfEhAKDExhbmd1YWdlX1NNShBgEhAKDExhbmd1YWdlX1NNThBhEg8KC0xhbmd1YWdlX1NREGISDwoLTGFuZ3VhZ2VfU1IQYxIPCgtMYW5ndWFnZV9TVhBkEg8KC0xhbmd1YWdlX1NXEGUSEAoMTGFuZ3VhZ2VfU1lSEGYSDwoLTGFuZ3VhZ2VfVEEQZxIPCgtMYW5ndWFnZV9URRBoEg8KC0xhbmd1YWdlX1RHEGkSDwoLTGFuZ3VhZ2VfVEgQahIPCgtMYW5ndWFnZV9USxBrEg8KC0xhbmd1YWdlX1ROEGwSDwoLTGFuZ3VhZ2VfVFIQbRIPCgtMYW5ndWFnZV9UVBBuEhAKDExhbmd1YWdlX1RaTRBvEg8KC0xhbmd1YWdlX1VHEHASDwoLTGFuZ3VhZ2VfVUsQcRIPCgtMYW5ndWFnZV9VUhByEg8KC0xhbmd1YWdlX1VaEHMSDwoLTGFuZ3VhZ2VfVkkQdBIPCgtMYW5ndWFnZV9XTxB1Eg8KC0xhbmd1YWdlX1hIEHYSDwoLTGFuZ3VhZ2VfWU8QdxIPCgtMYW5ndWFnZV9aSBB4Eg8KC0xhbmd1YWdlX1pVEHliBnByb3RvMw=='
      break;
    case 'video':
      templateName = 'tmpl_9705FC0B';
      descriptor = 'CuABCgdwLnByb3RvEhJvaXBQcm90by50ZW1wbGF0ZXMiuAEKAVASEwoLcHVibGlzaERhdGUYASABKAQSGAoQYWRkcmVzc0RpcmVjdG9yeRgDIAEoCRIQCghmaWxlbmFtZRgEIAEoCRITCgtkaXNwbGF5TmFtZRgFIAEoCRIZChF0aHVtYm5haWxGaWxlbmFtZRgGIAEoCSJCCgdOZXR3b3JrEg0KCVVOREVGSU5FRBAAEhAKDE5ldHdvcmtfSVBGUxABEhYKEk5ldHdvcmtfQklUVE9SUkVOVBACYgZwcm90bzM='
      break;
    case 'text':
      templateName = 'tmpl_769D8FBC';
      descriptor = 'CoYDCgdwLnByb3RvEhJvaXBQcm90by50ZW1wbGF0ZXMi3gIKAVASGAoHbmV0d29yaxgBIAEoDjIHTmV0d29yaxITCgt0ZXh0QWRkcmVzcxgCIAEoCRIiCgx0ZXh0RmlsZXR5cGUYAyABKA4yDFRleHRGaWxldHlwZRIRCglpc1ByZXZpZXcYBCABKAgibgoHTmV0d29yaxIVChFOZXR3b3JrX1VOREVGSU5FRBAAEhAKDE5ldHdvcmtfSVBGUxABEhYKEk5ldHdvcmtfQklUVE9SUkVOVBACEg8KC05ldHdvcmtfU0lBEAMSEQoNTmV0d29ya19TVE9SShAEIoIBCgxUZXh0RmlsZXR5cGUSGgoWVGV4dEZpbGV0eXBlX1VOREVGSU5FRBAAEhMKD1RleHRGaWxldHlwZV9NRBABEhQKEFRleHRGaWxldHlwZV9SVEYQAhIUChBUZXh0RmlsZXR5cGVfVFhUEAMSFQoRVGV4dEZpbGV0eXBlX0hUTUwQBGIGcHJvdG8z';
      break;
    case 'image':
      templateName = 'tmpl_1AC73C98';
      descriptor = 'CrwCCgdwLnByb3RvEhJvaXBQcm90by50ZW1wbGF0ZXMilAIKAVASGAoHbmV0d29yaxgBIAEoDjIHTmV0d29yaxIQCghmaWxlbmFtZRgCIAEoCRIUCgxpbWFnZUFkZHJlc3MYAyABKAkSGAoQdGh1bWJuYWlsQWRkcmVzcxgEIAEoCRIZCgt0YWtlbkJ5TGlzdBgFIAMoCzIEVHhpZBIbCg10YWtlbldpdGhMaXN0GAYgAygLMgRUeGlkEhoKDHByb3RvY29sTGlzdBgHIAMoCzIEVHhpZBoTCgRUeGlkEgsKA3JhdxgBIAEoDCJKCgdOZXR3b3JrEhUKEU5ldHdvcmtfVU5ERUZJTkVEEAASEAoMTmV0d29ya19JUEZTEAESFgoSTmV0d29ya19CSVRUT1JSRU5UEAJiBnByb3RvMw=='
      break;
    case 'article':
      templateName = 'tmpl_D019F2E1';
      descriptor = 'CpgCCgdwLnByb3RvEhJvaXBQcm90by50ZW1wbGF0ZXMi8AEKAVASGgoMYnlsaW5lV3JpdGVyGAEgASgLMgRUeGlkEhoKEmJ5bGluZVdyaXRlcnNUaXRsZRgCIAEoCRIdChVieWxpbmVXcml0ZXJzTG9jYXRpb24YAyABKAkSGQoLYXJ0aWNsZVRleHQYBCABKAsyBFR4aWQSFwoJaW1hZ2VMaXN0GAUgAygLMgRUeGlkEhgKEGltYWdlQ2FwdGlvbkxpc3QYBiADKAkSFwoJdmlkZW9MaXN0GAcgAygLMgRUeGlkEhgKEHZpZGVvQ2FwdGlvbkxpc3QYCCADKAkaEwoEVHhpZBILCgNyYXcYASABKAxiBnByb3RvMw=='
      break;
    case 'youtube':
      templateName = 'tmpl_834772F4';
      descriptor = 'CkoKB3AucHJvdG8SEm9pcFByb3RvLnRlbXBsYXRlcyIjCgFQEgsKA3VybBgBIAEoCRIRCgl5b3VUdWJlSWQYAiABKAliBnByb3RvMw=='
      break;
    default:
      break;
  }
  const templateInfo = {
    descriptor,
    name: templateName
  }
  return templateInfo
}

// format record data for publishing
async function formatRecord(recordData){
  const record = recordData.record;

  const article = {
    'basic': {
      'name': '',
      'description': '',
      'language': '',
      'date': '',
      'avatar': '',
      'tagList': '',
    },
    'article': {
      'bylineWriter': '',
      'bylineWritersTitle': '',
      'bylineWritersLocation': '',
      'articleText': '',
      'imageList': '', 
      'imageCaptionList': '', 
      'videoList': '', 
      'videoCaptionList': ''
    }
  }

  const text = {
    'basic': {
      // 'name': '',
      // 'description': '',
      'language': '',
      'date': '',
      // 'avatar': '',
      // 'tagList': ''
    },
    'text': {
      'textAddress': '',
      'isPreview': '',
      'textFiletype': '',
      'network': ''
    }
  }

  const textData = [text]
  textData[0].text.textAddress = record.textAddress || ''
  textData[0].text.isPreview = 'false';
  textData[0].text.textFiletype = (record.textFiletype === "TextFiletype_MD") ? 1 : null;
  textData[0].text.network = (record.network === "Network_IPFS") ? 1 : null;
  textData[0].basic.language = (record.language === "Language_EN") ? 23 : null;
  textData[0].basic.date = record.date || '';
  
  const embeddedImages = [];
  const embeddedImageAddresses = [];
  const embeddedImageCaptions = [];
  // if record.embeddedImages.length is undefined, then it's a text record
  // console.log('embeddedImages is not empty:', Array.isArray(record.embeddedImages))
  const embeddedImageQty = (Array.isArray(record.embeddedImages)) ? record.embeddedImages.length : 0;
  for (let i = 0; i < embeddedImageQty; i++) {
    image = {
      'basic': {
        'name': '',
        'description': '',
        'language': '',
        'date': '',
        'avatar': '',
        'tagList': ''
      },
      'image': {
        'filename': '',
        'imageAddress': '',
        'thumbnailAddress': '',
        'network': ''
      }
    }

    image.basic.name = record.embeddedImages[i].name || '';
    image.basic.description = record.embeddedImages[i].description || '';
    image.basic.language = (record.embeddedImages[i].language === "Language_EN") ? 23 : null;
    image.basic.avatar = record.embeddedImages[i].avatar || null;
    image.basic.tagList = record.embeddedImages[i].tagList || '';
    image.image.filename = record.embeddedImages[i].filename || '';
    image.image.imageAddress = record.embeddedImages[i].imageAddress || '';
    image.image.thumbnailAddress = record.embeddedImages[i].thumbnailAddress || '';
    image.image.network = (record.embeddedImages[i].network === "Network_IPFS") ? 1 : null;

    embeddedImages.push(image);
    embeddedImageAddresses.push(record.embeddedImages[i].imageAddress);
    embeddedImageCaptions.push(record.embeddedImages[i].caption);
  }

  let embeddedVideos = [];
  let embeddedVideocaptions = [];
  const embeddedVideoQty = Array.isArray(record.embeddedVideos) ? record.embeddedVideos.length : 0;
  for (let i = 0; i < embeddedVideoQty; i++) {
    video = {
      'basic': {
        'name': '',
        'description': '',
        'language': '',
        'date': '',
        'avatar': '',
        'tagList': '',
      },
      'video': {
        'filename': '',
        // 'p2PNetwork': '',
        'addressDirectory': '',
        'thumbnailFilename': '',
        'displayName': '',
        'publishDate': '',
      },
      'youtube': {
        'url': '',
        'youTubeId': '',
      }
    }
    
    video.basic.name = record.embeddedVideos[i].name || '';
    video.basic.description = record.embeddedVideos[i].description || '';
    video.basic.language = (record.embeddedVideos[i].language === "Language_EN") ? 23 : null;
    video.basic.date = record.embeddedVideos[i].date || '';
    video.basic.avatar = record.embeddedVideos[i].avatar || null;
    video.basic.tagList = record.embeddedVideos[i].tagList || '';
    video.video.filename = record.embeddedVideos[i].filename || '';
    video.video.addressDirectory = record.embeddedVideos[i].addressDirectory || '';
    video.video.thumbnailFilename = record.embeddedVideos[i].thumbnailFilename || '';
    video.video.displayName = record.embeddedVideos[i].displayName || '';
    video.video.publishDate = record.embeddedVideos[i].publishDate || '';
    video.youtube.url = record.embeddedVideos[i].youTubeURL || '';
    video.youtube.youTubeId = (video.youtube.url.startsWith("https://www.youtube.com")) ? video.youtube.url.split('?v=').pop() : video.youtube.url.split('youtu.be/').pop() || '';
    embeddedVideos.push(video)
    embeddedVideocaptions.push(record.embeddedVideos[i].caption);
  }

  const articleData = [article];
  articleData[0].basic.name = record.title || '';
  articleData[0].basic.description = record.description || '';
  articleData[0].basic.language = (record.language === "Language_EN") ? 23 : null;
  articleData[0].basic.date = record.date || '';
  articleData[0].basic.avatar = record.avatar || null;
  articleData[0].basic.tagList = record.tagList || '';
  articleData[0].article.bylineWriter = record.bylineWriter || '';
  articleData[0].article.bylineWritersTitle = record.bylineWritersTitle || '';
  articleData[0].article.bylineWritersLocation = record.bylineWritersLocation || '';
  articleData[0].article.imageList = embeddedImageAddresses || '';
  articleData[0].article.imageCaptionList = embeddedImageCaptions || '';
  articleData[0].article.videoCaptionList = embeddedVideocaptions || '';

  // console.log('articleData', articleData,'textData',textData,'embeddedVideos',embeddedVideos, 'embeddedImages',embeddedImages);
  const response = {
    'articleData': articleData,
    'textData': textData,
    'embeddedVideos': embeddedVideos,
    'embeddedImages': embeddedImages
  }
  return response
}

// get a record from the blockchain
async function getRecord(recordID) {
  let record
  try {
    record = await axios.get(`https://api.oip.io/oip/o5/record/get/${recordID}`);
  } catch (e) {
    console.log('error', e);
  }
  if (record.count === 0) {
    return null;
  }else {
  return record.data
  }
  
}


// --------------------------------------------------------------------------------
// endpoints

// use this endpoint to check the chain sync status and other info
app.get('/api/v1/getInfo', (req, res) => {
  getinfo().then(result => {
    let chainSync = (result.chain.progress * 100) + ' %';
    let chainIsSynced = (result.chain.progress == 1) ? true : false;
    res.send({
    "currentTime": new Date().toISOString(),
    "chain sync progress": chainSync,
    "chain synched": chainIsSynced,
    "info": result
    });
  });
  console.log("handling RPC call: getinfo");
});

// use this endpoint to get info about connected blockchain peers
app.get('/api/v1/getPeerInfo', (req, res) => {
  getPeerInfo().then(result => {
    res.send({
      "currentTime": new Date().toISOString(),
      "message": "All Systems Operational",
      "info": result
    });
  });
  console.log("handling RPC call: getpeerinfo");
});

// use this endpoint to get wallet's WIF (private key)
app.post('/api/v1/getWIF', (req, res) => {
  const body = req.body;
  const emailaddress = body.emailaddress;
  const pubKey = body.pubKey;
  const passphrase = body.options.passphrase || '';
  const id = SHA1(emailaddress).toString();
  if (!passphrase || passphrase.length === 0 ) {
    res.send({
      "currentTime": new Date().toISOString(),
      "message": "Error: Passphrase is required"
    });
  } else {
  getwif(id, pubKey, passphrase).then(result => {
    res.send({
      "currentTime": new Date().toISOString(),
      "message": "All Systems Operational",
      "wif": result
    });
  });
  console.log("handling RPC call: getWIF");
}
});

// use this endpoint to create a new wallet
app.post('/api/v1/createWallet', (req, res) => {
  console.log("handling RPC call: createWallet");
  const emailaddress = req.body.emailaddress || '';
  const options = req.body.options;
  const passphrase = options.passphrase;
  options.passphrase = "";
  const account = 'default';
  const id = req.body.id || SHA1(emailaddress).toString();
  if (!passphrase || passphrase.length === 0 ) {
    res.send({
      "currentTime": new Date().toISOString(),
      "message": "Error: Passphrase is required"
    });
  } else {
    createWallet(id, options).then(wallet => {//console.log('create wallet response:', wallet);
    if (wallet.toString().startsWith("Error")) {
        res.send({
          "currentTime": new Date().toISOString(),
          "message": wallet.toString()
        });
      } else {
        createAddress(id, account).then(address => {
          getMasterHDKey(id).then(masterHDKey => {
            getwif(id, address.address).then(wif => {
              selectwallet(id, address.address, wif).then(null1 => {
                encryptwallet(passphrase).then(null2 => {
                  getwalletinfo(id).then(walletinfo => {
                    res.send({
                      "currentTime": new Date().toISOString(),
                      "result": "Wallet Created Successfully",
                      "message": "emailAddress is the address associated with your walletID. pubKey is the address to send tokens to before you can publish. Please store your mnemonic, it will not be stored in our database and there is no way to ask for it again!",
                      "emailAddress": emailaddress,
                      "walletID": id,
                      "pubKey": address.address,
                      "encrypted": walletinfo.master.encrypted,
                      "mnemonic": masterHDKey.mnemonic.phrase,
                      "xprivkey": masterHDKey.key.xprivkey,
                      "walletinfo": walletinfo
                    });
                  });
                });
              });
            });
          });
        });
      }
    });
  }
});

// use this endpoint to generate a receiving address
app.post('/api/v1/generateReceivingAddress', (req, res) => {
  const id = req.body.id;
  // const options = req.body.options;
  const account = req.body.account;
  // console.log("id:",id, "account:", account);
  createAddress(id, account).then(result => {
    res.send({
      "currentTime": new Date().toISOString(),
      "message": "All Systems Operational",
      "send to this address": result.address,
      "info": result
    });
  });
  
  console.log("handling RPC call: createReceivingAddress");
});

// use this endpoint to get wallet info including balance
app.get('/api/v1/getWalletInfo', (req, res) => {
  const emailaddress = req.body.emailaddress || '';
  const id = req.body.id || SHA1(emailaddress).toString();
  const wallet = walletClient.wallet(id);
  wallet.getInfo(id).then(result => {
    res.send({
      "currentTime": new Date().toISOString(),
      "wallet id": id,
      "wallet balance": (result.balance.confirmed/100000000).toFixed(8),
      "info": result
    });
  });
  console.log("handling RPC call: getWalletInfo");
});

// use this endpoint to get wallet balance
app.get('/api/v1/getWalletBalance', (req, res) => {
  const emailaddress = req.body.emailaddress || '';
  const id = req.body.id || SHA1(emailaddress).toString();
  // const id = req.body.id || '';
  // console.log("id:",id);
  const wallet = walletClient.wallet(id);
  wallet.getInfo(id).then(result => {
    res.send({
      "currentTime": new Date().toISOString(),
      "wallet id": id,
      "wallet balance": (result.balance.confirmed/100000000).toFixed(8)
    });
  });
  console.log("handling RPC call: getWalletInfo");
});

// use this endpoint to get wallets history
app.get('/api/v1/getWalletTxHistory', (req, res) => {
  const id = req.body.id;
  console.log("id:",id);
  const wallet = walletClient.wallet(id);
  wallet.getAccounts(id).then(result => {
    console.log(result)
    wallet.getHistory(result).then(result => {
      res.send({
        "currentTime": new Date().toISOString(),
        "message": "All Systems Operational",
        // "wallet id": id,
        // "wallet balance": (result.balance.confirmed/100000000).toFixed(8),
        "info": result
      });
    });
  })

  
  console.log("handling RPC call: getWalletInfo");
});

// use this endpoint to register a publisher
app.post('/api/v1/registerPublisher', async (req, res) => {
  const authData = req.body[0];
  const publisherData = req.body[1];
  const emailaddress = authData.emailaddress || '';
  const passphrase = authData.passphrase || '';
  const account = 'default';
  const id = authData.id || SHA1(emailaddress).toString();
  const pubKey = publisherData.pubKey
  const publisherName = publisherData.name;
  
  try{
    const wif = await getwif(id, pubKey, passphrase)
    getFpub(id, account).then(fpub => { //console.log("fpub:", fpub)
      prepRegistration(pubKey, publisherName, fpub, wif).then(data => { //console.log('data:', data)
        getSignedP64FloData(data, wif).then(floData => { //console.log('floData:', floData)
          selectwallet(id, pubKey, wif).then(wallet => { //console.log('wallet:', wallet)
            walletpassphrase(passphrase).then(resX => {//console.log('resX:', resX)
              getwalletinfo().then(walletinfo => { //console.log('walletinfo:', walletinfo)
                sendFloDataToChain(floData, pubKey, wif).then(result => { //console.log("txidsX:", txids);
                  res.send({
                    "currentTime": new Date().toISOString(),
                    "message": "Publisher Registration Sent Successfully",
                    "Registration TXID": result.txid,
                    "Publisher Address": pubKey,
                    "Publisher Name": publisherName
                  });
                });
              });
            });
          });
        });
      });
    });
  }catch(e){
    console.log(e);
    res.send({
      "currentTime": new Date().toISOString(),
      "message": "Publisher Registration Failed",
      "cause": 'Incorrect Passphrase'
    })
  }
  
});

// use this endpoint to publish a record
app.post('/api/v1/publishRecord', async (req, res) => {
  const authData = req.body[0];
  const recordData = req.body[1];
  const emailaddress = authData.emailaddress || '';
  const passphrase = authData.passphrase || '';
  const account = 'default';
  const id = authData.id || SHA1(emailaddress).toString();

  formattedRecordData = await formatRecord(recordData)
  formattedTextData = formattedRecordData.textData;
  formattedArticleData = formattedRecordData.articleData;
  formattedEmbeddedImages = formattedRecordData.embeddedImages || [];
  formattedEmbeddedVideos = formattedRecordData.embeddedVideos || [];
  embeddedVideoQty = formattedRecordData.embeddedVideos.length || 0;
  embeddedImageQty = formattedEmbeddedImages.length || 0;

  const pubKey = recordData.pubKey;
  try{
    wif = await getwif(id, pubKey, passphrase);
  } catch(err) {
    console.log('Error getting wif', err);
    res.status(500).send({
      "currentTime": new Date().toISOString(),
      "message": "Publish Record Failed",
      "cause": 'Incorrect Passphrase'
    });
    return;
  }
  
  fpub = await getFpub(id, account);
  wallet = await selectwallet(id, pubKey, wif);
  unlock = await walletpassphrase(passphrase);
  walletinfo = await getwalletinfo();
  let recordDataX = "";

  // first we check if there are any embedded videos and if so we loop thru them and create a new record for each one
  // then we publish the text data record
  // then we publish the article data with the text data record and embedded video records as references
  let embeddedVideoTXIDs = [];

  if (embeddedVideoQty > 0) {
    let embeddedVideoTXs = [];
    for (let i = 0; i < embeddedVideoQty; i++) {
      const basic = formattedEmbeddedVideos[i].basic
      const video = formattedEmbeddedVideos[i].video
      const youtube = formattedEmbeddedVideos[i].youtube
      const payload = [basic, video, youtube];
      let templates = ['basic','video','youtube'];
      let data = [];
      for (let i = 0; i < templates.length; i++) {
        let templateType = templates[i];
        let templateInfo = await getTemplateInfo(templateType);
        let templateDescriptor = templateInfo.descriptor;
        let templateName = templateInfo.name;
        let templatePayload = payload[i]
        let template = {
          descriptor: templateDescriptor,
          name: templateName,
          payload: templatePayload
        }
        data.push(template);
      }
      function makeRecord(data) {
        return {
          details: data,
          myMainAddress: wif,
          pubKey: pubKey
        }
      }

      recordDataX = makeRecord(data);
      signed64 = await getSignedP64FloData(recordDataX, wif);
      wallet = await selectwallet(id, pubKey, wif);
      walletinfo = await getwalletinfo();
      walletdata = await walletData(pubKey, signed64);

  if (signed64.length > 1040) {
    let mpx = new Modules.MultipartX(signed64).multiparts;

    if (!Array.isArray(mpx)) {
      return console.log('uh oh', mpx);
    }
    // mpx
    signatureData1 = await prepareFirstTXofMP(id, pubKey, wif, mpx);
    sig1 = await signMessage(wif, pubKey, signatureData1);
    floData1 = await getFloDataForFirstTXofMP(id, pubKey, wif, mpx, sig1);
    sendFloDataToChain(floData1, pubKey, wif).then(referenceTxO => {
      sendRestOfTXsOfMP(id, pubKey, wif, mpx, referenceTxO).then(recordTxidArray => {
          console.log('recordTxidArray', recordTxidArray);
          embeddedVideoTXIDs.push(referenceTxO.txid);
          console.log('video reference record', embeddedVideoTXIDs);
          res.send({
            "current time": new Date().toISOString(),
              "message": "Published Successfully",
              "reference txid": referenceTxO.txid
            });
          })
      })
  } else {
    // single
    const delay = ms => new Promise(res => setTimeout(res, ms));
      await delay(2000);
      if (i > 0) {
        let previousTransactionOutput = embeddedVideoTXs
        sendFloDataToChain(signed64, pubKey, wif, previousTransactionOutput).then(referenceTxO => {
          embeddedVideoTXIDs.push(referenceTxO.txid)
          embeddedVideoTXs.push(referenceTxO);
        // console.log('video reference records', embeddedVideoTXIDs);
        // res.send({
        //         "current time": new Date().toISOString(),
        //           "message": "Published Successfully",
        //           "record txid": referenceTxO.txid
        // });
        })
      } else {
      sendFloDataToChain(signed64, pubKey, wif).then(referenceTxO => {
        embeddedVideoTXIDs.push(referenceTxO.txid)
        embeddedVideoTXs.push(referenceTxO);
        // console.log('video reference record', referenceTxO.txid);
        // res.send({
        //         "current time": new Date().toISOString(),
        //           "message": "Published Successfully",
        //           "record txid": referenceTxO.txid
        // });
      })
    }
    
  }
  }
  } else {
    // no embedded videos
    console.log('no embedded videos, finish this part');
  }
  const delay = ms => new Promise(res => setTimeout(res, ms));
  await delay(2000);

  // next we publish the embedded images
  let embeddedImageTXIDs = [];
  let embeddedImageTXs = [];

  if (embeddedImageQty > 0) {
    for (let i = 0; i < embeddedImageQty; i++) {
      const image = formattedEmbeddedImages[i].image
      const basic = formattedEmbeddedImages[i].basic   
      const payload = [basic, image];
      let templates = ['basic','image'];
      let imageData = [];
      for (let ii = 0; ii < templates.length; ii++) {
        let templateType = templates[ii];
        let templateInfo = await getTemplateInfo(templateType);
        let templateDescriptor = templateInfo.descriptor;
        let templateName = templateInfo.name;
        let templatePayload = payload[ii]
        let imageTemplate = {
          descriptor: templateDescriptor,
          name: templateName,
          payload: templatePayload
        }
        imageData.push(imageTemplate);
      }
      function makeImageRecord(imageData) {
        return {
          details: imageData,
          myMainAddress: wif,
          pubKey: pubKey
        }
      }

      recordDataX = makeImageRecord(imageData);
      signed64 = await getSignedP64FloData(recordDataX, wif);
      wallet = await selectwallet(id, pubKey, wif);
      walletinfo = await getwalletinfo();
      walletdata = await walletData(pubKey, signed64);

  if (signed64.length > 1040) {
    let mpx = new Modules.MultipartX(signed64).multiparts;

    if (!Array.isArray(mpx)) {
      return console.log('uh oh', mpx);
    }
    // mpx
    signatureData1 = await prepareFirstTXofMP(id, pubKey, wif, mpx);
    sig1 = await signMessage(wif, pubKey, signatureData1);
    floData1 = await getFloDataForFirstTXofMP(id, pubKey, wif, mpx, sig1);
    sendFloDataToChain(floData1, pubKey, wif).then(referenceTxO => {
      sendRestOfTXsOfMP(id, pubKey, wif, mpx, referenceTxO).then(recordTxidArray => {
          console.log('recordTxidArray', recordTxidArray);
          embeddedImageTXIDs.push(referenceTxO.txid);
          console.log('image reference record', embeddedImageTXIDs);
          res.send({
            "current time": new Date().toISOString(),
              "message": "Published Successfully",
              "reference txid": referenceTxO.txid,
              "record txids":recordTxidArray
            });
          })
      })
  } else {
    // single
    const delay = ms => new Promise(res => setTimeout(res, ms));
      await delay(2000);
    if (i > 0) {
      
      let previousTransactionOutput = embeddedImageTXs
      // console.log('previous transaction output this loop',i, previousTransactionOutput)
      sendFloDataToChain(signed64, pubKey, wif, previousTransactionOutput).then(referenceTxO => {
      // console.log('referenceTxO', referenceTxO);
      embeddedImageTXIDs.push(referenceTxO.txid)
      embeddedImageTXs.push(referenceTxO);
      // console.log('image reference records', embeddedImageTXIDs);
      // res.send({
      //         "current time": new Date().toISOString(),
      //           "message": "Published Successfully",
      //           "record txid": referenceTxO.txid
      // });
    })
    } else {
    sendFloDataToChain(signed64, pubKey, wif).then(referenceTxO => {
      // console.log('referenceTxO', referenceTxO);
      embeddedImageTXIDs.push(referenceTxO.txid)
      embeddedImageTXs.push(referenceTxO);
      // console.log('image reference record', referenceTxO.txid);
      // res.send({
      //         "current time": new Date().toISOString(),
      //           "message": "Published Successfully",
      //           "record txid": referenceTxO.txid
      // });

    })
    }
    
    }
  }
  } else {
    // no embedded videos
    console.log('no embedded images, finish this part');
  }
  await delay(2000);

  // next we publish the text data record
  let embeddedTextTX = [];
  let embeddedTextTXID = [];
  let text = formattedTextData[0].text
  let basic = formattedTextData[0].basic
  let payload = [basic, text]; 
  let templates = ['basic', 'text'];
  let data = [];
  for (let i = 0; i < templates.length; i++) {
    let templateType = templates[i];
    let templateInfo = await getTemplateInfo(templateType);
    let templateDescriptor = templateInfo.descriptor;
    let templateName = templateInfo.name;
    let templatePayload = payload[i]
    template = {
      descriptor: templateDescriptor,
      name: templateName,
      payload: templatePayload
    }
    data.push(template);
  }

  // console.log('investigate this',template, data)
  function makeTextRecord(template) { // this is fucking wierd, it shoudln't work
  // function makeTextRecord(data) { // this should work, but it doesn't
    // console.log('investigate this too',template, data)
    return {
      details: data,
      myMainAddress: wif,
      pubKey: pubKey
    }
  }
  
  let recordDataImage = makeTextRecord(template);
  signed64 = await getSignedP64FloData(recordDataImage, wif);
  wallet = await selectwallet(id, pubKey, wif);
  walletinfo = await getwalletinfo();
  walletdata = await walletData(pubKey, signed64);

  if (signed64.length > 1040) {
    // mpx
    let mpx = new Modules.MultipartX(signed64).multiparts;

    if (!Array.isArray(mpx)) {
      return console.log('uh oh', mpx);
    }
    
    signatureData1 = await prepareFirstTXofMP(id, pubKey, wif, mpx);
    sig1 = await signMessage(wif, pubKey, signatureData1);
    floData1 = await getFloDataForFirstTXofMP(id, pubKey, wif, mpx, sig1);
    sendFloDataToChain(floData1, pubKey, wif).then(referenceTxO => {
      sendRestOfTXsOfMP(id, pubKey, wif, mpx, referenceTxO).then(recordTxidArray => {
        embeddedTextTXID.push(referenceTxO.txid);
      })
    })
  } else {
    // single
    const delay = ms => new Promise(res => setTimeout(res, ms));
    await delay(2000);

    sendFloDataToChain(signed64, pubKey, wif).then(referenceTxO => {
      embeddedTextTXID.push(referenceTxO.txid)
      embeddedTextTX.push(referenceTxO);
    })
  }
  await delay(2000);

  // and now we publish the article with the reference to the video and text
  let article = formattedArticleData[0].article
  basic = formattedArticleData[0].basic
  article.videoList = embeddedVideoTXIDs
  article.articleText = embeddedTextTXID[0]
  article.imageList = embeddedImageTXIDs
  let embeddedArticleTXID = [];

  // next we publish the text data record  
  payload = [basic, article];  
  templates = ['basic','article'];
  data = [];
  for (let z = 0; z < templates.length; z++) {
    let templateType = templates[z];
    let templateInfo = await getTemplateInfo(templateType);
    let templateDescriptor = templateInfo.descriptor;
    let templateName = templateInfo.name;
    let templatePayload = payload[z]
    let template = {
      descriptor: templateDescriptor,
      name: templateName,
      payload: templatePayload
    }
    data.push(template);
  }
  // console.log('data', data);
  function makeMasterRecord(data) {
    return {
      details: data,
      myMainAddress: wif,
      pubKey: pubKey
    }
  }

  recordDataMaster = makeMasterRecord(data);
  signed64 = await getSignedP64FloData(recordDataMaster, wif);
  wallet = await selectwallet(id, pubKey, wif);
  walletinfo = await getwalletinfo();
  walletdata = await walletData(pubKey, signed64);

  if (signed64.length > 1040) {
    // mpx
    let mpx = new Modules.MultipartX(signed64).multiparts;

    if (!Array.isArray(mpx)) {
      return console.log('uh oh', mpx);
    }

    signatureData1 = await prepareFirstTXofMP(id, pubKey, wif, mpx);
    sig1 = await signMessage(wif, pubKey, signatureData1);
    floData1 = await getFloDataForFirstTXofMP(id, pubKey, wif, mpx, sig1);
    sendFloDataToChain(floData1, pubKey, wif).then(referenceTxO => {
      sendRestOfTXsOfMP(id, pubKey, wif, mpx, referenceTxO).then(recordTxidArray => {
        // console.log('recordTxidArray', recordTxidArray);
        embeddedArticleTXID.push(referenceTxO.txid);
        console.log('article reference record', embeddedArticleTXID);
        res.send({
          "current time": new Date().toISOString(),
          "message": "Published Successfully",
          "reference txid": referenceTxO.txid
        });
      })
    })
  } else {
  // single
    const delay = ms => new Promise(res => setTimeout(res, ms));
    await delay(2000);

    sendFloDataToChain(signed64, pubKey, wif).then(referenceTxO => {
      embeddedArticleTXID.push(referenceTxO.txid)
      console.log('article reference record', referenceTxO.txid);
      res.send({
        "current time": new Date().toISOString(),
        "message": "Published Successfully",
        "article record txid": referenceTxO.txid
      });
    })
  }
}) 

// use this endpoint to get a record
app.get('/api/v1/getRecord/:recordID', async (req, res) => {
  const recordID = req.params.recordID;
  try{
    const record = await getRecord(recordID);
    const testIfRecord = record.results[0].record.details
      res.send({
        "currentTime": new Date().toISOString(),
        "message": "Record Found",
        "Record": record.results
      });
  } catch (e) {
    if (e) {
      res.send({
        "currentTime": new Date().toISOString(),
        "message": "Record Not Found",
        "recordID": recordID
      });
    }
  }
})

// use this endpoint to get a record and expand all OIPRefs in it to their full records
app.get('/api/v1/getExpandedRecord/:recordID', async (req, res) => {
  const recordID = req.params.recordID;
  try{
    const mainRecord = await getRecord(recordID);
    const bylineWriter = mainRecord.results[0].record.details.tmpl_D019F2E1.bylineWriter;
    const embeddedImages = mainRecord.results[0].record.details.tmpl_D019F2E1.imageList;
    const embeddedVideos = mainRecord.results[0].record.details.tmpl_D019F2E1.videoList;
    const articleText = mainRecord.results[0].record.details.tmpl_D019F2E1.articleText;
    bylineWriterIsRef = (Buffer.from(bylineWriter).length === 64) ? true : false;
    articleTextIsRef = (Buffer.from(articleText).length === 64) ? true : false;
    embeddedImagesIsRef = (Array.isArray(embeddedImages)) ? (Buffer.from(embeddedImages[0]).length === 64) : false;
    embeddedVideosIsRef = (Array.isArray(embeddedVideos)) ? (Buffer.from(embeddedVideos[0]).length === 64) : false;
    const txids = {
      bylineWriter: (bylineWriterIsRef) ? bylineWriter : '',
      articleText: (articleTextIsRef) ? articleText : '',
      embeddedImages: (embeddedImagesIsRef) ? embeddedImages : [],
      embeddedVideos: (embeddedVideosIsRef) ? embeddedVideos : []
    };
    const bylineWriterRecord = await getRecord(txids.bylineWriter);
    const bylineWriterResults = bylineWriterRecord.results
    const articleTextRecord = await getRecord(txids.articleText)
    const articleTextResults = articleTextRecord.results
    let videoRecords = [];
    for(let i = 0; i < txids.embeddedVideos.length; i++) {
      const video = txids.embeddedVideos[i];
      const videoRecord = await getRecord(video);
      const videoRecordResults = videoRecord.results;
      videoRecords.push(videoRecordResults);
    }
    let imageRecords = [];
    for(let i = 0; i < txids.embeddedImages.length; i++) {
      const image = txids.embeddedImages[i];
      const imageRecord = await getRecord(image);
      const imageRecordResults = imageRecord.results;
      imageRecords.push(imageRecordResults);
    }
    Object.defineProperties(mainRecord.results[0].record.details.tmpl_D019F2E1, {
      bylineWriter: {
        value: bylineWriterResults
      },
      videoList: {
        value: videoRecords
      },
      imageList: {
        value: imageRecords
      },
      articleText: {
        value: articleTextResults
      }
    });
    res.send({
        "currentTime": new Date().toISOString(),
        "message": "Record Found",
        "Record": mainRecord.results
    });
  } catch (e) {
    // console.log('error', e);
    if (e) {
      res.send({
        "currentTime": new Date().toISOString(),
        "message": "Record Not Found",
        "recordID": recordID
      });
    }
  }
})