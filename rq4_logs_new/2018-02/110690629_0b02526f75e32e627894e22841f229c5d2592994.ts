export const abi =[
	{
		"constant": true,
		"inputs": [
			{
				"name": "",
				"type": "uint256"
			}
		],
		"name": "retrieveCost",
		"outputs": [
			{
				"name": "",
				"type": "uint256"
			}
		],
		"payable": false,
		"stateMutability": "view",
		"type": "function"
	},
	{
		"constant": true,
		"inputs": [
			{
				"name": "",
				"type": "address"
			},
			{
				"name": "",
				"type": "uint256"
			}
		],
		"name": "playerNumberOfBoost",
		"outputs": [
			{
				"name": "",
				"type": "uint256"
			}
		],
		"payable": false,
		"stateMutability": "view",
		"type": "function"
	},
	{
		"constant": true,
		"inputs": [],
		"name": "owner",
		"outputs": [
			{
				"name": "",
				"type": "address"
			}
		],
		"payable": false,
		"stateMutability": "view",
		"type": "function"
	},
	{
		"anonymous": false,
		"inputs": [
			{
				"indexed": true,
				"name": "",
				"type": "address"
			},
			{
				"indexed": false,
				"name": "idBoost",
				"type": "uint256"
			},
			{
				"indexed": false,
				"name": "numberOfBoost",
				"type": "uint256"
			}
		],
		"name": "Purchase",
		"type": "event"
	},
	{
		"constant": false,
		"inputs": [
			{
				"name": "_idBoost",
				"type": "uint256"
			},
			{
				"name": "_cost",
				"type": "uint256"
			}
		],
		"name": "createOrModifBoost",
		"outputs": [],
		"payable": false,
		"stateMutability": "nonpayable",
		"type": "function"
	},
	{
		"constant": false,
		"inputs": [
			{
				"name": "_address",
				"type": "address"
			},
			{
				"name": "_value",
				"type": "uint256"
			},
			{
				"name": "_token",
				"type": "address"
			},
			{
				"name": "_extraData",
				"type": "bytes"
			}
		],
		"name": "receiveApproval",
		"outputs": [],
		"payable": false,
		"stateMutability": "nonpayable",
		"type": "function"
	},
	{
		"inputs": [
			{
				"name": "_nexiumAdress",
				"type": "address"
			}
		],
		"payable": false,
		"stateMutability": "nonpayable",
		"type": "constructor"
	}
]