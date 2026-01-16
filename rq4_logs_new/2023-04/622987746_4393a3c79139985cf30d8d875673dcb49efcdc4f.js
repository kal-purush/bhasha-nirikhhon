const ethers = require('ethers'),
  web3 = require('web3')
const net = require('net'),
  readlineSync = require('readline-sync'),
  env = require('./config.json')
Object.assign(process.env, env)
let ethersprovider,
  web3provider,
  web3subscription = '',
  wallet,
  account,
  mainContractAddress = process.env.mainContract,
  subprivatekey = process.env.subprivatekey.split(','),
  subWallet = [],
  subAccount = [],
  wbnbAddress = '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2',
  usdtAddress = '0xdAC17F958D2ee523a2206206994597C13D831ec7',
  routerAddress = '0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D',
  tokenAddress = '0x',
  pairAddress = '0x',
  pathAddress = [],
  sellPathAddress = [],
  devAddress,
  connectionType = process.env.connectionType,
  gasLimit = process.env.gasLimit,
  gasPriceMultiplier = process.env.gasPriceMultiplier,
  maxBuyTax = process.env.maxBuyTax,
  antiHP
if (process.env.antiHP === 'true') {
  antiHP = true
} else {
  process.env.antiHP === 'false' && (antiHP = false)
}
let mainContract,
  routerContract,
  subRouterContract = [],
  tokenContract,
  subTokenContract = [],
  mainContractABI = [
    'function swapExactETHForTokens(uint buyAmount,address[] calldata pathAddress,address[] calldata walletAddress,uint buyRepeat,uint maxBuyTax,bool antiHP) external payable',
    'function swapETHForExactTokens(uint tokenOut,uint bnbInMax,address[] calldata pathAddress,address[] calldata walletAddress,uint buyRepeat,uint maxBuyTax,bool antiHP) external payable',
    'function user(address) public view returns (bool)',
  ],
  routerContractABI = [
    'function swapExactETHForTokens(uint amountOutMin, address[] calldata path, address to, uint deadline) external payable',
    'function swapETHForExactTokens(uint amountOut, address[] calldata path, address to, uint deadline)external payable returns (uint[] memory amounts)',
    'function swapExactTokensForETHSupportingFeeOnTransferTokens(uint amountIn, uint amountOutMin, address[] calldata path, address to, uint deadline) external',
    'function getAmountsOut(uint amountIn, address[] memory path) public view returns (uint[] memory amounts)',
  ],
  tokenContractABI = [
    'function name() external pure returns (string memory)',
    'function owner() public view returns (address)',
    'function getOwner() external view returns (address)',
    'function balanceOf(address account) external view returns (uint256)',
    'function decimals() view returns (uint8)',
    'function allowance(address owner, address spender) external view returns (uint)',
    'function approve(address _spender, uint256 _value) public returns (bool success)',
    'function transferFrom(address sender, address recipient, uint256 amount) external returns (bool)',
  ],
  recipients = [],
  buyAmount,
  buyRepeat,
  sendValue,
  buyDelay
let skipBlock,
  tokenName,
  tokenDecimals,
  method,
  toSell = [],
  targetPrice,
  sendAmount,
  ExactETHContract,
  ExactETHRouter
async function connect() {
  if (connectionType === 'ipc') {
    web3provider = new web3(
      new web3.providers.IpcProvider(process.env.ipc_node, net)
    )
    ethersprovider = new ethers.providers.WebSocketProvider(process.env.ws_node)
  } else {
    if (connectionType === 'ws') {
      ethersprovider = new ethers.providers.WebSocketProvider(
        process.env.ws_node
      )
    }
  }
  wallet = new ethers.Wallet(process.env.privatekey)
  account = wallet.connect(ethersprovider)
  mainContract = new ethers.Contract(
    mainContractAddress,
    mainContractABI,
    account
  )
  routerContract = new ethers.Contract(
    routerAddress,
    routerContractABI,
    account
  )
  if (tokenAddress === '0x') {
  } else {
    tokenContract = new ethers.Contract(tokenAddress, tokenContractABI, account)
  }
  for (let _0x114039 = 0; _0x114039 < subprivatekey.length; _0x114039++) {
    subWallet[_0x114039] = new ethers.Wallet(subprivatekey[_0x114039])
    subAccount[_0x114039] = subWallet[_0x114039].connect(ethersprovider)
    subRouterContract[_0x114039] = new ethers.Contract(
      routerAddress,
      routerContractABI,
      subAccount[_0x114039]
    )
    if (tokenAddress === '0x') {
    } else {
      subTokenContract[_0x114039] = new ethers.Contract(
        tokenAddress,
        tokenContractABI,
        subAccount[_0x114039]
      )
    }
  }
}
async function mainMenu() {
  console.clear()
  web3subscription = ''
  await connect()
  await unsubs()
  console.log('EVE')
  console.log('Connection: ' + connectionType)
  console.log(
    'mainWallet: ' +
      account.address +
      ' | ' +
      (await ethersprovider.getBalance(account.address)) * 1e-18 +
      ' ETH'
  )
  console.log('subWallet: ' + subprivatekey.length)
  console.log('MaxBuyTax: ' + maxBuyTax + ' AntiHP: ' + antiHP)
  console.log(
    'gasLimit: ' + gasLimit + ' gasPriceMultiplier: ' + gasPriceMultiplier
  )
  console.log('\n1. Snipe Menu')
  console.log('2. Direct Buy Menu')
  console.log('3. Sell Menu')
  console.log('5. Approve Token')
  console.log('6. Check Token')
  console.log('7. Check subWallet')
  console.log('8. Transfer ETH from mainWallet to subWallet')
  console.log('9. Eve Settings')
  let _0x2561d6 = readlineSync.question('\nSelect Menu : ')
  if (_0x2561d6 === '1') {
    await connect()
    await selectWallet()
    await selectPair()
    await inputSnipeData()
    await setPath()
    await connect()
    await getName()
    await getOwner()
    return snipeMenu()
  } else {
    if (_0x2561d6 === '2') {
      return (
        await connect(), await selectWallet(), await selectPair(), directMenu()
      )
    } else {
      if (_0x2561d6 === '3') {
        return sellMenu()
      } else {
        if (_0x2561d6 === '5') {
          await connect()
          for (
            let _0x504a34 = 0;
            _0x504a34 < subprivatekey.length;
            _0x504a34++
          ) {
            console.log(
              'Wallet[' + _0x504a34 + ']' + subAccount[_0x504a34].address
            )
          }
          return await selectSellWallet(), approveToken()
        } else {
          if (_0x2561d6 === '6') {
            return (
              await checkToken(),
              readlineSync.question('Press Enter To Continue'),
              mainMenu()
            )
          } else {
            if (_0x2561d6 === '7') {
              console.clear()
              console.log('subWallet on your setting.')
              for (
                let _0x303a03 = 0;
                _0x303a03 < subprivatekey.length;
                _0x303a03++
              ) {
                console.log(
                  subAccount[_0x303a03].address +
                    ' | ' +
                    (await ethersprovider.getBalance(
                      subAccount[_0x303a03].address
                    )) *
                      1e-18 +
                    ' ETH'
                )
              }
              return (
                readlineSync.question('Press Enter To Continue'), mainMenu()
              )
            } else {
              if (_0x2561d6 === '8') {
                await connect()
                await selectWallet()
                let _0x37277a = readlineSync.question('Amount: ')
                return (
                  (sendAmount = ethers.utils.parseUnits(
                    _0x37277a.toString(),
                    'ether'
                  )),
                  sendBNB()
                )
              } else {
                if (_0x2561d6 === '9') {
                  return eveSettings()
                }
              }
            }
          }
        }
      }
    }
  }
}
async function snipeMenu() {
  console.clear()
  console.log('CONTRACT EXACT ETH')
  console.log('1. Snipe Liquidity Exact ETH(Contract)')
  console.log('2. Snipe Custom Hex Exact ETH(Contract)')
  console.log('ROUTER EXACT ETH')
  console.log('3. Snipe Liquidity Exact ETH(Router)')
  console.log('4. Snipe Custom Hex Exact ETH(Router)')
  console.log('\n5. Back To Main Menu')
  let _0xed5be6 = readlineSync.question('\nSelect : ')
  if (_0xed5be6 === '1') {
    return (
      await connect(),
      (ExactETHContract = true),
      (ExactETHRouter = false),
      listenLiquidityExactETHContract()
    )
  } else {
    if (_0xed5be6 === '2') {
      await connect()
      ExactETHContract = true
      ExactETHRouter = false
      method = readlineSync.question('Method: ')
      return listenCustomExactETHContract()
    } else {
      if (_0xed5be6 === '3') {
        return (
          await connect(),
          (ExactETHContract = false),
          (ExactETHRouter = true),
          listenLiquidityExactETHRouter()
        )
      } else {
        if (_0xed5be6 === '4') {
          await connect()
          ExactETHContract = false
          ExactETHRouter = true
          method = readlineSync.question('Method: ')
          return listenCustomExactETHRouter()
        } else {
          if (_0xed5be6 === '5') {
            return mainMenu()
          }
        }
      }
    }
  }
}
async function directMenu() {
  console.log('1. Direct Buy Exact ETH(Contract)')
  console.log('2. Direct Buy Exact ETH(Router)')
  console.log('\n5. Back To Main Menu')
  let _0x44eafa = readlineSync.question('\nSelect : ')
  if (_0x44eafa === '1') {
    await connect()
    await inputSnipeData()
    await setPath()
    let _0x544fc0 = await ethersprovider.getFeeData()
    return buyExactETHContract(_0x544fc0.gasPrice)
  } else {
    if (_0x44eafa === '2') {
      await connect()
      await inputSnipeData()
      await setPath()
      let _0x77fd94 = await ethersprovider.getFeeData()
      return buyExactETHRouter(_0x77fd94.gasPrice)
    } else {
      if (_0x44eafa === '5') {
        return mainMenu()
      }
    }
  }
}
async function selectWallet() {
  await connect()
  recipients = []
  console.log('Select Wallet :')
  for (let _0x13d472 = 0; _0x13d472 < subprivatekey.length; _0x13d472++) {
    console.log('Wallet[' + _0x13d472 + ']' + subAccount[_0x13d472].address)
  }
  let _0x59aa49 = readlineSync.question('Choose Wallet : ')
  if (_0x59aa49.length > subprivatekey.length) {
    console.log('Selected Wallets :')
    for (let _0x386642 = 0; _0x386642 < subprivatekey.length; _0x386642++) {
      recipients[_0x386642] = subAccount[_0x386642].address
    }
    console.log(recipients)
  } else {
    if (_0x59aa49.length === subprivatekey.length) {
      console.log('Selected Wallets :')
      for (let _0x50ad00 = 0; _0x50ad00 < subprivatekey.length; _0x50ad00++) {
        recipients[_0x50ad00] = subAccount[_0x50ad00].address
      }
      console.log(recipients)
    } else {
      console.log('Selected Wallets :')
      for (let _0x45c409 = 0; _0x45c409 < _0x59aa49.length; _0x45c409++) {
        recipients[_0x45c409] = subAccount[_0x59aa49[_0x45c409]].address
      }
      console.log(recipients)
    }
  }
}
async function selectPair() {
  await connect()
  pathAddress = []
  if (pairAddress === '0x') {
    let _0x970b9d = readlineSync.question(
      'Select Pair\n1.WETH\n2.USDT\n3.Custom\nSelect : '
    )
    if (_0x970b9d === '1') {
      pairAddress = wbnbAddress
    } else {
      if (_0x970b9d === '2') {
        pairAddress = usdtAddress
      } else {
        _0x970b9d === '3' &&
          (pairAddress = readlineSync.question('Input custom pair : '))
      }
    }
  } else {
    let _0x3e197d = new ethers.Contract(pairAddress, tokenContractABI, account),
      _0x3c0a75 = await _0x3e197d.name(),
      _0x16269b = readlineSync.question(
        '\n1.Previous Pair(' + _0x3c0a75 + ') 2.New Pair\nSelect : '
      )
    if (_0x16269b === '1') {
      pairAddress = pairAddress
    } else {
      if (_0x16269b === '2') {
        let _0x142eb2 = readlineSync.question(
          'Select Pair\n1.WETH\n2.USDT\n3.Custom\nSelect : '
        )
        if (_0x142eb2 === '1') {
          pairAddress = wbnbAddress
        } else {
          if (_0x142eb2 === '2') {
            pairAddress = usdtAddress
          } else {
            if (_0x142eb2 === '3') {
              pairAddress = readlineSync.question('Input custom pair : ')
            }
          }
        }
      }
    }
  }
}
async function inputSnipeData() {
  let _0x102b0c = readlineSync.question('EthOut: ')
  buyAmount = ethers.utils.parseUnits(_0x102b0c.toString(), 'ether')
  buyRepeat = readlineSync.question('buyRepat: ')
  sendValue =
    buyAmount * buyRepeat * recipients.length +
    (buyAmount * buyRepeat * recipients.length * 2) / 100 +
    100000000000000
  skipBlock = readlineSync.question('skipBlock: ')
  buyDelay = readlineSync.question('buyDelay(ms): ')
  tokenAddress = readlineSync.question('tokenAddress: ')
}
async function buyExactETHContract(_0xeef33e, _0x2768fa, _0x3a709e) {
  if (gasPriceMultiplier > 1) {
    try {
      let _0x38109b = await mainContract.swapExactETHForTokens(
        buyAmount,
        pathAddress,
        recipients,
        buyRepeat,
        maxBuyTax,
        antiHP,
        {
          gasLimit: gasLimit * buyRepeat * recipients.length,
          gasPrice: _0xeef33e * gasPriceMultiplier,
          value: sendValue.toString(),
        }
      )
      return (
        console.log('https://etherscan.io/tx/' + _0x38109b.hash),
        await unsubs(),
        readlineSync.question('Press Enter To Continue'),
        mainMenu()
      )
    } catch (_0x1eda81) {
      return (
        console.log(_0x1eda81),
        await unsubs(),
        readlineSync.question('Press Enter To Continue'),
        mainMenu()
      )
    }
  } else {
    try {
      let _0x1215cf = await mainContract.swapExactETHForTokens(
        buyAmount,
        pathAddress,
        recipients,
        buyRepeat,
        maxBuyTax,
        antiHP,
        {
          gasLimit: gasLimit * buyRepeat * recipients.length,
          maxPriorityFeePerGas: _0x2768fa,
          maxFeePerGas: _0x3a709e,
          value: sendValue.toString(),
        }
      )
      return (
        console.log('https://etherscan.io/tx/' + _0x1215cf.hash),
        await unsubs(),
        readlineSync.question('Press Enter To Continue'),
        mainMenu()
      )
    } catch (_0x43ee7f) {
      return (
        console.log(_0x43ee7f),
        await unsubs(),
        readlineSync.question('Press Enter To Continue'),
        mainMenu()
      )
    }
  }
}
async function buyExactETHRouter(_0x494620, _0x3ec36c, _0xfc38fb) {
  if (gasPriceMultiplier > 1) {
    try {
      for (let _0x47be3b = 0; _0x47be3b < buyRepeat; _0x47be3b++) {
        for (let _0x430c98 = 0; _0x430c98 < recipients.length; _0x430c98++) {
          let _0x145a97 = await routerContract.swapExactETHForTokens(
            0,
            pathAddress,
            recipients[_0x430c98],
            Math.floor(Date.now() / 1000 + 60).toString(),
            {
              gasLimit: gasLimit,
              gasPrice: _0x494620 * gasPriceMultiplier,
              value: buyAmount,
            }
          )
          console.log(_0x145a97.hash)
        }
      }
      return (
        await unsubs(),
        readlineSync.question('Press Enter To Continue'),
        mainMenu()
      )
    } catch (_0xe08e00) {
      return (
        console.log(_0xe08e00),
        await unsubs(),
        readlineSync.question('Press Enter To Continue'),
        mainMenu()
      )
    }
  } else {
    try {
      for (let _0x503a4a = 0; _0x503a4a < buyRepeat; _0x503a4a++) {
        for (let _0x4e0fdf = 0; _0x4e0fdf < recipients.length; _0x4e0fdf++) {
          const _0x3e7abb = {
            gasLimit: gasLimit,
            maxPriorityFeePerGas: _0x3ec36c,
            maxFeePerGas: _0xfc38fb,
            value: buyAmount,
          }
          let _0x5b049e = await routerContract.swapExactETHForTokens(
            0,
            pathAddress,
            recipients[_0x4e0fdf],
            Math.floor(Date.now() / 1000 + 60).toString(),
            _0x3e7abb
          )
          console.log(_0x5b049e.hash)
        }
      }
      return (
        await unsubs(),
        readlineSync.question('Press Enter To Continue'),
        mainMenu()
      )
    } catch (_0x3860c5) {
      return (
        console.log(_0x3860c5),
        await unsubs(),
        readlineSync.question('Press Enter To Continue'),
        mainMenu()
      )
    }
  }
}
async function listenLiquidityExactETHContract() {
  if (connectionType === 'ipc') {
    console.log('Listening addLiquidityETH (IPC)')
    web3subscription = web3provider.eth.subscribe(
      'pendingTransactions',
      async (_0x2cddf4, _0x45d2cd) => {
        await web3provider.eth
          .getTransaction(_0x45d2cd)
          .then(async (_0x4a6d47) => {
            if (
              _0x4a6d47 != null &&
              _0x4a6d47.from.toLowerCase() === devAddress.toLowerCase()
            ) {
              if (
                _0x4a6d47.input.includes('0xf305d719') ||
                _0x4a6d47.input.includes('0xe8e33700')
              ) {
                if (skipBlock < 1) {
                  return setTimeout(
                    () =>
                      buyExactETHContract(
                        _0x4a6d47.gasPrice,
                        _0x4a6d47.maxPriorityFeePerGas,
                        _0x4a6d47.maxFeePerGas
                      ),
                    buyDelay
                  )
                } else {
                  let _0x2acaa6 = await ethersprovider.getBlockNumber()
                  return (
                    console.log(
                      '\ndevAddress Transaction Detected at Block: ' + _0x2acaa6
                    ),
                    await unsubs(),
                    checkBlock(
                      _0x2acaa6,
                      _0x4a6d47.gasPrice,
                      _0x4a6d47.maxPriorityFeePerGas,
                      _0x4a6d47.maxFeePerGas
                    )
                  )
                }
              }
            }
          })
      }
    )
  } else {
    connectionType === 'ws' &&
      (console.log('Listening addLiquidityETH (WS)'),
      ethersprovider.on('pending', async (_0x5449a1) => {
        ethersprovider.getTransaction(_0x5449a1).then(async (_0x2e48f9) => {
          if (
            _0x2e48f9 != null &&
            _0x2e48f9.from.toLowerCase() === devAddress.toLowerCase()
          ) {
            if (
              _0x2e48f9.data.includes('0xf305d719') ||
              _0x2e48f9.data.includes('0xe8e33700')
            ) {
              if (skipBlock < 1) {
                return setTimeout(
                  () =>
                    buyExactETHContract(
                      _0x2e48f9.gasPrice,
                      _0x2e48f9.maxPriorityFeePerGas,
                      _0x2e48f9.maxFeePerGas
                    ),
                  buyDelay
                )
              } else {
                let _0x5b8231 = await ethersprovider.getBlockNumber()
                return (
                  console.log(
                    '\ndevAddress Transaction Detected at Block: ' + _0x5b8231
                  ),
                  await unsubs(),
                  checkBlock(
                    _0x5b8231,
                    _0x2e48f9.gasPrice,
                    _0x2e48f9.maxPriorityFeePerGas,
                    _0x2e48f9.maxFeePerGas
                  )
                )
              }
            }
          }
        })
      }))
  }
}
async function listenCustomExactETHContract() {
  if (connectionType === 'ipc') {
    console.log('Listening ' + method + ' (IPC)')
    web3subscription = web3provider.eth.subscribe(
      'pendingTransactions',
      async (_0x574c90, _0x5c5bd9) => {
        await web3provider.eth
          .getTransaction(_0x5c5bd9)
          .then(async (_0x1751fa) => {
            if (
              _0x1751fa != null &&
              _0x1751fa.from.toLowerCase() === devAddress.toLowerCase() &&
              _0x1751fa.input.includes(method)
            ) {
              if (skipBlock < 1) {
                return setTimeout(
                  () =>
                    buyExactETHContract(
                      _0x1751fa.gasPrice,
                      _0x1751fa.maxPriorityFeePerGas,
                      _0x1751fa.maxFeePerGas
                    ),
                  buyDelay
                )
              } else {
                let _0x34b581 = await ethersprovider.getBlockNumber()
                return (
                  console.log(
                    '\ndevAddress Transaction Detected at Block: ' + _0x34b581
                  ),
                  await unsubs(),
                  checkBlock(
                    _0x34b581,
                    _0x1751fa.gasPrice,
                    _0x1751fa.maxPriorityFeePerGas,
                    _0x1751fa.maxFeePerGas
                  )
                )
              }
            }
          })
      }
    )
  } else {
    if (connectionType === 'ws') {
      console.log('Listening ' + method + ' (WS)')
      ethersprovider.on('pending', async (_0x1d4a21) => {
        ethersprovider.getTransaction(_0x1d4a21).then(async (_0x322dc6) => {
          if (
            _0x322dc6 != null &&
            _0x322dc6.from.toLowerCase() === devAddress.toLowerCase() &&
            _0x322dc6.data.includes(method)
          ) {
            if (skipBlock < 1) {
              return setTimeout(
                () =>
                  buyExactETHContract(
                    _0x322dc6.gasPrice,
                    _0x322dc6.maxPriorityFeePerGas,
                    _0x322dc6.maxFeePerGas
                  ),
                buyDelay
              )
            } else {
              let _0x2bc4a1 = await ethersprovider.getBlockNumber()
              return (
                console.log(
                  '\ndevAddress Transaction Detected at Block: ' + _0x2bc4a1
                ),
                await unsubs(),
                checkBlock(
                  _0x2bc4a1,
                  _0x322dc6.gasPrice,
                  _0x322dc6.maxPriorityFeePerGas,
                  _0x322dc6.maxFeePerGas
                )
              )
            }
          }
        })
      })
    }
  }
}
async function listenLiquidityExactETHRouter() {
  if (connectionType === 'ipc') {
    console.log('Listening addLiquidityETH (IPC)')
    web3subscription = web3provider.eth.subscribe(
      'pendingTransactions',
      async (_0x11bc66, _0x4df84a) => {
        await web3provider.eth
          .getTransaction(_0x4df84a)
          .then(async (_0x530f3b) => {
            if (
              _0x530f3b != null &&
              _0x530f3b.from.toLowerCase() === devAddress.toLowerCase()
            ) {
              if (
                _0x530f3b.input.includes('0xf305d719') ||
                _0x530f3b.input.includes('0xe8e33700')
              ) {
                if (skipBlock < 1) {
                  return setTimeout(
                    () =>
                      buyExactETHRouter(
                        _0x530f3b.gasPrice,
                        _0x530f3b.maxPriorityFeePerGas,
                        _0x530f3b.maxFeePerGas
                      ),
                    buyDelay
                  )
                } else {
                  let _0x625348 = await ethersprovider.getBlockNumber()
                  return (
                    console.log(
                      '\ndevAddress Transaction Detected at Block: ' + _0x625348
                    ),
                    await unsubs(),
                    checkBlock(
                      _0x625348,
                      _0x530f3b.gasPrice,
                      _0x530f3b.maxPriorityFeePerGas,
                      _0x530f3b.maxFeePerGas
                    )
                  )
                }
              }
            }
          })
      }
    )
  } else {
    connectionType === 'ws' &&
      (console.log('Listening addLiquidityETH (WS)'),
      ethersprovider.on('pending', async (_0x1c317c) => {
        ethersprovider.getTransaction(_0x1c317c).then(async (_0x524465) => {
          if (
            _0x524465 != null &&
            _0x524465.from.toLowerCase() === devAddress.toLowerCase()
          ) {
            if (
              _0x524465.data.includes('0xf305d719') ||
              _0x524465.data.includes('0xe8e33700')
            ) {
              if (skipBlock < 1) {
                return setTimeout(
                  () =>
                    buyExactETHRouter(
                      _0x524465.gasPrice,
                      _0x524465.maxPriorityFeePerGas,
                      _0x524465.maxFeePerGas
                    ),
                  buyDelay
                )
              } else {
                let _0x18518c = await ethersprovider.getBlockNumber()
                return (
                  console.log(
                    '\ndevAddress Transaction Detected at Block: ' + _0x18518c
                  ),
                  await unsubs(),
                  checkBlock(
                    _0x18518c,
                    _0x524465.gasPrice,
                    _0x524465.maxPriorityFeePerGas,
                    _0x524465.maxFeePerGas
                  )
                )
              }
            }
          }
        })
      }))
  }
}
async function listenCustomExactETHRouter() {
  if (connectionType === 'ipc') {
    console.log('Listening ' + method + ' (IPC)')
    web3subscription = web3provider.eth.subscribe(
      'pendingTransactions',
      async (_0x4ac3e0, _0x4565cb) => {
        await web3provider.eth
          .getTransaction(_0x4565cb)
          .then(async (_0x729013) => {
            if (
              _0x729013 != null &&
              _0x729013.from.toLowerCase() === devAddress.toLowerCase() &&
              _0x729013.input.includes(method)
            ) {
              if (skipBlock < 1) {
                return setTimeout(
                  () =>
                    buyExactETHRouter(
                      _0x729013.gasPrice,
                      _0x729013.maxPriorityFeePerGas,
                      _0x729013.maxFeePerGas
                    ),
                  buyDelay
                )
              } else {
                let _0xbeb74d = await ethersprovider.getBlockNumber()
                return (
                  console.log(
                    '\ndevAddress Transaction Detected at Block: ' + _0xbeb74d
                  ),
                  await unsubs(),
                  checkBlock(
                    _0xbeb74d,
                    _0x729013.gasPrice,
                    _0x729013.maxPriorityFeePerGas,
                    _0x729013.maxFeePerGas
                  )
                )
              }
            }
          })
      }
    )
  } else {
    connectionType === 'ws' &&
      (console.log('Listening ' + method + ' (WS)'),
      ethersprovider.on('pending', async (_0x2db828) => {
        ethersprovider.getTransaction(_0x2db828).then(async (_0x54e2a2) => {
          if (
            _0x54e2a2 != null &&
            _0x54e2a2.from.toLowerCase() === devAddress.toLowerCase() &&
            _0x54e2a2.data.includes(method)
          ) {
            if (skipBlock < 1) {
              return setTimeout(
                () =>
                  buyExactETHRouter(
                    _0x54e2a2.gasPrice,
                    _0x54e2a2.maxPriorityFeePerGas,
                    _0x54e2a2.maxFeePerGas
                  ),
                buyDelay
              )
            } else {
              let _0x57eb45 = await ethersprovider.getBlockNumber()
              return (
                console.log(
                  '\ndevAddress Transaction Detected at Block: ' + _0x57eb45
                ),
                await unsubs(),
                checkBlock(
                  _0x57eb45,
                  _0x54e2a2.gasPrice,
                  _0x54e2a2.maxPriorityFeePerGas,
                  _0x54e2a2.maxFeePerGas
                )
              )
            }
          }
        })
      }))
  }
}
async function sellMenu() {
  await connect()
  console.clear()
  await checkToken()
  console.log('\n0. Refresh')
  console.log('1. Sell When Dev Remove Liquitidy')
  console.log('2. Sell When Dev Call Custom Method')
  console.log('3. Sell When Price Reach Target')
  console.log('4. Direct Sell')
  console.log('5. Back To Main Menu')
  let _0x36c80a = readlineSync.question('\nSelect Menu:')
  if (_0x36c80a === '0') {
    return sellMenu()
  } else {
    if (_0x36c80a === '1') {
      return await getOwner(), await selectSellWallet(), listenRemoveLiquidity()
    } else {
      if (_0x36c80a === '2') {
        return (
          await getOwner(),
          await selectSellWallet(),
          (method = readlineSync.question('Method :')),
          listenCustomSell()
        )
      } else {
        if (_0x36c80a === '3') {
          await selectSellWallet()
          targetPrice = readlineSync.question('Target Price: ')
          await getPrice()
          let _0x5e3c7c = await ethersprovider.getFeeData()
          return sellToken(_0x5e3c7c.gasPrice)
        } else {
          if (_0x36c80a === '4') {
            await selectSellWallet()
            let _0x4a78a8 = await ethersprovider.getFeeData()
            return sellToken(_0x4a78a8.gasPrice)
          } else {
            if (_0x36c80a === '5') {
              return mainMenu()
            }
          }
        }
      }
    }
  }
}
async function listenRemoveLiquidity() {
  console.log('Listening Dev Remove Liquidity (WS)')
  ethersprovider.on('pending', async (_0x148f25) => {
    ethersprovider.getTransaction(_0x148f25).then(async (_0x251434) => {
      if (
        _0x251434 != null &&
        _0x251434.from.toLowerCase() === devAddress.toLowerCase()
      ) {
        if (
          _0x251434.data.includes('0x02751cec') ||
          _0x251434.data.includes('0x2195995c') ||
          _0x251434.data.includes('0x5b0d5984') ||
          _0x251434.data.includes('0xaf2979eb') ||
          _0x251434.data.includes('0xbaa2abde') ||
          _0x251434.data.includes('0xded9382a')
        ) {
          return (
            console.log('Dev is trigger remove liquidity.'),
            sellToken(_0x251434.gasPrice * 2)
          )
        }
      }
    })
  })
}
async function listenCustomSell() {
  console.log('Listening Dev Trigger ' + method + ' (WS)')
  ethersprovider.on('pending', async (_0x549266) => {
    ethersprovider.getTransaction(_0x549266).then(async (_0x108b25) => {
      if (
        _0x108b25 != null &&
        _0x108b25.from.toLowerCase() === devAddress.toLowerCase() &&
        _0x108b25.data.includes(method)
      ) {
        return (
          console.log('Dev is trigger ' + method),
          sellToken(_0x108b25.gasPrice * 2)
        )
      }
    })
  })
}
async function selectSellWallet() {
  await connect()
  toSell = []
  console.log('Select wallet to sell')
  let _0xf184e4 = readlineSync.question('Choose Wallet : ')
  if (_0xf184e4.length > subprivatekey.length) {
    console.log(
      'Wallet you choosed exceed the wallet you have, sell on all wallet.'
    )
    for (let _0x1c8cad = 0; _0x1c8cad < subprivatekey.length; _0x1c8cad++) {
      toSell[_0x1c8cad] = _0xf184e4[_0x1c8cad]
      console.log(subRouterContract[_0x1c8cad].signer.address)
    }
  } else {
    if (_0xf184e4.length === subprivatekey.length) {
      console.log('Sell on all wallet.')
      for (let _0x5b8709 = 0; _0x5b8709 < subprivatekey.length; _0x5b8709++) {
        toSell[_0x5b8709] = _0xf184e4[_0x5b8709]
        console.log(subRouterContract[_0x5b8709].signer.address)
      }
    } else {
      console.log('Selected wallet')
      for (let _0x4f2fe8 = 0; _0x4f2fe8 < _0xf184e4.length; _0x4f2fe8++) {
        toSell[_0x4f2fe8] = _0xf184e4[_0x4f2fe8]
        console.log(subRouterContract[_0xf184e4[_0x4f2fe8]].signer.address)
      }
    }
  }
}
async function sellToken(_0x154153) {
  for (let _0x49e0f0 = 0; _0x49e0f0 < toSell.length; _0x49e0f0++) {
    try {
      let _0x294e56 = await tokenContract.balanceOf(
        subAccount[toSell[_0x49e0f0]].address
      )
      const _0x4778e0 = {
        gasLimit: gasLimit,
        gasPrice: _0x154153,
      }
      let _0x48beef = await subRouterContract[
        toSell[_0x49e0f0]
      ].swapExactTokensForETHSupportingFeeOnTransferTokens(
        _0x294e56,
        0,
        sellPathAddress,
        account.address,
        Math.floor(Date.now() / 1000 + 60).toString(),
        _0x4778e0
      )
      console.log(
        'Wallet[' + _0x49e0f0 + '] https://etherscan.io/tx/' + _0x48beef.hash
      )
    } catch (_0x1822b1) {
      console.log(_0x1822b1)
    }
  }
  return (
    await unsubs(), readlineSync.question('Press Enter To Continue'), mainMenu()
  )
}
async function eveSettings() {
  console.clear()
  console.log('1. Set Buy Tax')
  console.log('2. Toogle Anti HP')
  console.log('3. Set gasLimit')
  console.log('4. Set gasPrice Multipler')
  console.log('5. Change Connection Type')
  console.log('6. Back To Main Menu')
  let _0x2a71b2 = readlineSync.question('\nSelect Menu : ')
  if (_0x2a71b2 === '1') {
    console.clear()
    console.log('Current Max Buy Tax : ' + maxBuyTax)
    maxBuyTax = readlineSync.question('\nNew Max Buy Tax : ')
    readlineSync.question('Press Enter To Continue')
    return mainMenu()
  } else {
    if (_0x2a71b2 === '2') {
      return (
        console.clear(),
        antiHP
          ? (console.log('Disabling AntiHP'), (antiHP = false))
          : (console.log('Enabling AntiHP'), (antiHP = true)),
        readlineSync.question('Press Enter To Continue'),
        mainMenu()
      )
    } else {
      if (_0x2a71b2 === '3') {
        return (
          console.clear(),
          console.log('Current gasLimit : ' + gasLimit),
          (gasLimit = readlineSync.question('\nNew gasLimit : ')),
          readlineSync.question('Press Enter To Continue'),
          mainMenu()
        )
      } else {
        if (_0x2a71b2 === '4') {
          console.clear()
          console.log('Current gasPriceMultiplier : ' + gasPriceMultiplier)
          gasPriceMultiplier = readlineSync.question(
            '\nNew gasPriceMultiplier : '
          )
          readlineSync.question('Press Enter To Continue')
          return mainMenu()
        } else {
          if (_0x2a71b2 === '5') {
            console.clear()
            console.log('Current Connection Type : ' + connectionType)
            let _0x1d0dba = readlineSync.question(
              '\nSelect New connectionType :\n1.IPC\n2.WS\nSelect: '
            )
            if (_0x1d0dba === '1') {
              connectionType = 'ipc'
            } else {
              _0x1d0dba === '2' && (connectionType = 'ws')
            }
            return (
              console.log('connectionType Changed Into ' + connectionType),
              readlineSync.question('Press Enter To Continue'),
              mainMenu()
            )
          } else {
            if (_0x2a71b2 === '6') {
              return mainMenu()
            }
          }
        }
      }
    }
  }
}
async function getPrice() {
  let _0x16c437 = await tokenContract.balanceOf(subAccount[0].address)
  let _0x4e90ce = await routerContract.getAmountsOut(_0x16c437, sellPathAddress)
  if (targetPrice <= ethers.utils.formatUnits(_0x4e90ce[1], 18)) {
    return false
  } else {
    return (
      console.log(
        'Target Price: ' +
          targetPrice +
          ' current Price: ' +
          ethers.utils.formatUnits(_0x4e90ce[1], 18)
      ),
      getPrice()
    )
  }
}
async function checkToken() {
  await selectPair()
  let _0x2deb71
  if (tokenAddress === '0x') {
    tokenAddress = readlineSync.question('\nToken Address : ')
  } else {
    await getName()
    let _0x453f87 = readlineSync.question(
      '\n1.Previous Contract(' + tokenName + ') 2.New Contract\nSelect : '
    )
    if (_0x453f87 === '1') {
      tokenAddress = tokenAddress
    } else {
      _0x453f87 === '2' &&
        (tokenAddress = readlineSync.question('\nToken Address : '))
    }
  }
  await setPath()
  await connect()
  await getName()
  tokenDecimals = await tokenContract.decimals()
  console.log('subWallet Token Balance')
  for (let _0x1fa0c4 = 0; _0x1fa0c4 < subprivatekey.length; _0x1fa0c4++) {
    let _0x376061 = await tokenContract.balanceOf(subAccount[_0x1fa0c4].address)
    try {
      _0x2deb71 = await routerContract.getAmountsOut(_0x376061, sellPathAddress)
    } catch (_0x5a9a2e) {
      _0x2deb71 = [0, 0]
    }
    console.log(
      'Wallet[' +
        _0x1fa0c4 +
        ']' +
        subAccount[_0x1fa0c4].address +
        '|' +
        ethers.utils.formatUnits(_0x376061, tokenDecimals) +
        ' ' +
        tokenName +
        '|' +
        ethers.utils.formatUnits(_0x2deb71[1], 18) +
        ' BNB'
    )
  }
}
async function getOwner() {
  await connect()
  try {
    devAddress = await tokenContract.owner()
    console.log('\ndevAddress: ' + devAddress)
  } catch (_0x58558b) {
    try {
      devAddress = await tokenContract.getOwner()
      console.log('\ndevAddress: ' + devAddress)
    } catch (_0x36394f) {
      console.log('\ndevAddress not found. Please manual input.')
    }
  }
  let _0x3e0fda = readlineSync.question(
    '\nIs devAddress correct ?\n1.Yes\n2.No(Input Manual)\nChoose: '
  )
  if (_0x3e0fda === '1') {
    console.log('\ndevAddress to listen: ' + devAddress)
  } else {
    _0x3e0fda === '2' &&
      ((devAddress = readlineSync.question('New devAddress: ')),
      console.log('\ndevAddress to listen: ' + devAddress))
  }
}
async function getName() {
  await connect()
  try {
    tokenName = await tokenContract.name()
    tokenDecimals = await tokenContract.decimals()
    console.log('Loaded ' + tokenName + ' Decimals ' + tokenDecimals)
  } catch (_0x588723) {
    ;(tokenName = 'Undefined'),
      (tokenDecimals = '0'),
      console.log('Loaded ' + tokenName + ' Decimals ' + tokenDecimals)
  }
}
async function checkBlock(_0x37d74b, _0x1a06ba, _0x3c640d, _0x27be87) {
  let _0x1b2914 = await ethersprovider.getBlockNumber(),
    _0xfb3dee = parseInt(_0x37d74b) + parseInt(skipBlock)
  if (_0x1b2914 < _0xfb3dee) {
    return (
      console.log(
        'devBlock: ' +
          _0x37d74b +
          '|skipBlock: ' +
          skipBlock +
          '|buyBlock: ' +
          _0xfb3dee +
          '|currrentBlock: ' +
          _0x1b2914
      ),
      checkBlock(_0x37d74b, _0x1a06ba, _0x3c640d, _0x27be87)
    )
  } else {
    console.log('\nbuyBlock Reached.')
    console.log('buyBlock: ' + _0xfb3dee + '|currentBlock: ' + _0x1b2914)
    if (ExactETHContract) {
      return buyExactETHContract(_0x1a06ba, _0x3c640d, _0x27be87)
    } else {
      if (ExactETHRouter) {
        return buyExactETHRouter(_0x1a06ba, _0x3c640d, _0x27be87)
      }
    }
  }
}
async function approveToken() {
  await connect()
  let _0x2cd0ff = []
  for (let _0x94cf5a = 0; _0x94cf5a < toSell.length; _0x94cf5a++) {
    _0x2cd0ff[_0x94cf5a] = await subTokenContract[toSell[_0x94cf5a]].allowance(
      subAccount[_0x94cf5a].address,
      routerAddress
    )
    if (_0x2cd0ff[_0x94cf5a]['_hex'] === '0x00') {
      let _0x4808c8 = await subTokenContract[toSell[_0x94cf5a]].approve(
        routerAddress,
        ethers.constants.MaxUint256
      )
      console.log(
        'Wallet[' +
          subAccount[_0x94cf5a].address +
          '] https://etherscan.io/tx/' +
          _0x4808c8.hash
      )
    } else {
      console.log(
        'Wallet[' + subAccount[_0x94cf5a].address + '] Already Approved'
      )
    }
  }
  readlineSync.question('Press Enter To Continue')
  return mainMenu()
}
async function sendBNB() {
  await connect()
  for (let _0x2b2231 = 0; _0x2b2231 < recipients.length; _0x2b2231++) {
    const _0x4772d4 = {
      to: recipients[_0x2b2231],
      value: sendAmount,
    }
    let _0xc9fcd8 = await account.sendTransaction(_0x4772d4)
    console.log(
      'Wallet[' +
        recipients[_0x2b2231] +
        ']https://etherscan.io/tx/' +
        _0xc9fcd8.hash
    )
  }
  readlineSync.question('Press Enter To Continue')
  return mainMenu()
}
async function setPath() {
  if (pairAddress === wbnbAddress) {
    pathAddress = [wbnbAddress, tokenAddress]
    sellPathAddress = [tokenAddress, wbnbAddress]
  } else {
    pathAddress = [wbnbAddress, pairAddress, tokenAddress]
    sellPathAddress = [tokenAddress, pairAddress, wbnbAddress]
  }
}
async function unsubs() {
  if (connectionType === 'ipc') {
    if (web3subscription === '') {
    } else {
      web3subscription.unsubscribe()
      ethersprovider.removeAllListeners()
    }
  } else {
    connectionType === 'ws' && ethersprovider.removeAllListeners()
  }
}
mainMenu()