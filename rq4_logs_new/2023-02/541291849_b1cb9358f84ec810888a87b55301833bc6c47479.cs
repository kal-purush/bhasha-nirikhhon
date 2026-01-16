using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Newtonsoft.Json;

public class BidContract : MonoBehaviour
{
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    public async void auctionBid()
    {
        // string chain = "ethereum";
        // set network mainnet, testnet
        // string network = "goerli";
        // smart contract method to call
        // string method = "bid";
        // // abi in json form
        // // address of contract
        // string contract = "0x598d7Ba48C368D13408E7a46c8171659e1915297";
        // // array of arguments for contract
        // string value = "30";
        // // array of arguments for contract you can also add a nonce here as optional parameter
        // string[] obj = {value};
        // // string[] obj = {""};
        // string args = JsonConvert.SerializeObject(obj);
        // // string args = "[]";
        // // connects to user's browser wallet to call a transaction
        // string response = await Web3GL.SendContract(method, abi, contract, args, "30", "", "");
        // // connects to user's browser wallet to call a transaction
        // // string response = await EVM.Call(contract, abi, value, method, args);
        // // display response in game
        // print(response);

        string method = "bid";
        // abi in json format
        string abi = "[ { \"inputs\": [ { \"internalType\": \"address\", \"name\": \"_nft\", \"type\": \"address\" }, { \"internalType\": \"uint256\", \"name\": \"_nftId\", \"type\": \"uint256\" }, { \"internalType\": \"uint256\", \"name\": \"_startingBid\", \"type\": \"uint256\" }, { \"internalType\": \"address\", \"name\": \"to\", \"type\": \"address\" } ], \"stateMutability\": \"nonpayable\", \"type\": \"constructor\" }, { \"anonymous\": false, \"inputs\": [ { \"indexed\": true, \"internalType\": \"address\", \"name\": \"sender\", \"type\": \"address\" }, { \"indexed\": false, \"internalType\": \"uint256\", \"name\": \"amount\", \"type\": \"uint256\" } ], \"name\": \"Bid\", \"type\": \"event\" }, { \"anonymous\": false, \"inputs\": [ { \"indexed\": false, \"internalType\": \"address\", \"name\": \"winner\", \"type\": \"address\" }, { \"indexed\": false, \"internalType\": \"uint256\", \"name\": \"amount\", \"type\": \"uint256\" } ], \"name\": \"End\", \"type\": \"event\" }, { \"anonymous\": false, \"inputs\": [], \"name\": \"Start\", \"type\": \"event\" }, { \"anonymous\": false, \"inputs\": [ { \"indexed\": true, \"internalType\": \"address\", \"name\": \"bidder\", \"type\": \"address\" }, { \"indexed\": false, \"internalType\": \"uint256\", \"name\": \"amount\", \"type\": \"uint256\" } ], \"name\": \"Withdraw\", \"type\": \"event\" }, { \"inputs\": [ { \"internalType\": \"uint256\", \"name\": \"amount\", \"type\": \"uint256\" }, { \"internalType\": \"address\", \"name\": \"bidder\", \"type\": \"address\" } ], \"name\": \"bid\", \"outputs\": [], \"stateMutability\": \"payable\", \"type\": \"function\" }, { \"inputs\": [ { \"internalType\": \"address\", \"name\": \"\", \"type\": \"address\" } ], \"name\": \"bids\", \"outputs\": [ { \"internalType\": \"uint256\", \"name\": \"\", \"type\": \"uint256\" } ], \"stateMutability\": \"view\", \"type\": \"function\" }, { \"inputs\": [], \"name\": \"end\", \"outputs\": [], \"stateMutability\": \"payable\", \"type\": \"function\" }, { \"inputs\": [], \"name\": \"endAt\", \"outputs\": [ { \"internalType\": \"uint256\", \"name\": \"\", \"type\": \"uint256\" } ], \"stateMutability\": \"view\", \"type\": \"function\" }, { \"inputs\": [], \"name\": \"ended\", \"outputs\": [ { \"internalType\": \"bool\", \"name\": \"\", \"type\": \"bool\" } ], \"stateMutability\": \"view\", \"type\": \"function\" }, { \"inputs\": [], \"name\": \"highestBid\", \"outputs\": [ { \"internalType\": \"uint256\", \"name\": \"\", \"type\": \"uint256\" } ], \"stateMutability\": \"view\", \"type\": \"function\" }, { \"inputs\": [], \"name\": \"highestBidder\", \"outputs\": [ { \"internalType\": \"address\", \"name\": \"\", \"type\": \"address\" } ], \"stateMutability\": \"view\", \"type\": \"function\" }, { \"inputs\": [], \"name\": \"nft\", \"outputs\": [ { \"internalType\": \"contract IERC721\", \"name\": \"\", \"type\": \"address\" } ], \"stateMutability\": \"view\", \"type\": \"function\" }, { \"inputs\": [], \"name\": \"nftId\", \"outputs\": [ { \"internalType\": \"uint256\", \"name\": \"\", \"type\": \"uint256\" } ], \"stateMutability\": \"view\", \"type\": \"function\" }, { \"inputs\": [], \"name\": \"seller\", \"outputs\": [ { \"internalType\": \"address payable\", \"name\": \"\", \"type\": \"address\" } ], \"stateMutability\": \"view\", \"type\": \"function\" }, { \"inputs\": [ { \"internalType\": \"address\", \"name\": \"from\", \"type\": \"address\" } ], \"name\": \"start\", \"outputs\": [], \"stateMutability\": \"payable\", \"type\": \"function\" }, { \"inputs\": [], \"name\": \"started\", \"outputs\": [ { \"internalType\": \"bool\", \"name\": \"\", \"type\": \"bool\" } ], \"stateMutability\": \"view\", \"type\": \"function\" }, { \"inputs\": [], \"name\": \"withdraw\", \"outputs\": [], \"stateMutability\": \"nonpayable\", \"type\": \"function\" } ]";
        // address of contract
        string contract = "0xDb62C105DD7aD9bEb0719b1E88FeBCB9B9483C58";
        // array of arguments for contract
        string value = "15000";
        // array of arguments for contract you can also add a nonce here as optional parameter
        string bidder = "0xb4cDf4f2674B81b79BA944b256Dafc7b7885aB0B";
        string[] obj = {value,bidder};

        // string[] obj = {""};
        string args = JsonConvert.SerializeObject(obj);
        Debug.Log(args);
        // string args = "[]";
        // string args = "[msg.value,msg.sender]";
        // object[] args = new object[] { (uint)12000, to};
        // string args = "[12000,0x7EFb396d9cf6060e089576B394d5c571BCcD4291]";
        // connects to user's browser wallet to call a transaction
        string response = await Web3GL.SendContract(method, abi, contract, args, "0", "", "");
        // connects to user's browser wallet to call a transaction
        Debug.Log(response);
        // string response = await EVM.Call(chain, network, contract, abi, value, method, args);
        // display response in game
        print(response);
    }
}