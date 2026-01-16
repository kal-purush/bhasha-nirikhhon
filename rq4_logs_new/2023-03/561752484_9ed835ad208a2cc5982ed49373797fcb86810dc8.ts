/* eslint-disable */

export const protobufPackage = 'vega'

/** A deposit for a Vega built-in asset */
export interface BuiltinAssetDeposit {
  /** A Vega network internal asset identifier */
  vegaAssetId: string
  /** A Vega party identifier (pub-key) */
  partyId: string
  /**
   * The amount to be deposited
   * This field is an unsigned integer passed as a string and needs to be scaled using the asset's decimal places.
   */
  amount: string
}

/** A withdrawal for a Vega built-in asset */
export interface BuiltinAssetWithdrawal {
  /** A Vega network internal asset identifier */
  vegaAssetId: string
  /** A Vega network party identifier (pub-key) */
  partyId: string
  /**
   * The amount to be withdrawn
   * This field is an unsigned integer passed as a string and needs to be scaled using the asset's decimal places.
   */
  amount: string
}

/** An event related to a Vega built-in asset */
export interface BuiltinAssetEvent {
  /** Built-in asset deposit */
  deposit?: BuiltinAssetDeposit | undefined
  /** Built-in asset withdrawal */
  withdrawal?: BuiltinAssetWithdrawal | undefined
}

/** An asset allow-listing for an ERC20 token */
export interface ERC20AssetList {
  /** The Vega network internal identifier of the asset */
  vegaAssetId: string
  /** The ethereum address of the asset */
  assetSource: string
}

/** An asset deny-listing for an ERC20 token */
export interface ERC20AssetDelist {
  /** The Vega network internal identifier of the asset */
  vegaAssetId: string
}

export interface ERC20AssetLimitsUpdated {
  /** The Vega network internal identifier of the asset */
  vegaAssetId: string
  /** The Ethereum wallet that initiated the deposit */
  sourceEthereumAddress: string
  /** The updated lifetime limits */
  lifetimeLimits: string
  /** The updated withdrawal threshold */
  withdrawThreshold: string
}

/** An asset deposit for an ERC20 token */
export interface ERC20Deposit {
  /** The vega network internal identifier of the asset */
  vegaAssetId: string
  /** The Ethereum wallet that initiated the deposit */
  sourceEthereumAddress: string
  /** The Vega party identifier (pub-key) which is the target of the deposit */
  targetPartyId: string
  /** The amount to be deposited */
  amount: string
}

/** An asset withdrawal for an ERC20 token */
export interface ERC20Withdrawal {
  /** The Vega network internal identifier of the asset */
  vegaAssetId: string
  /** The target Ethereum wallet address */
  targetEthereumAddress: string
  /** The reference nonce used for the transaction */
  referenceNonce: string
}

/** An event related to an ERC20 token */
export interface ERC20Event {
  /** Index of the log in the transaction */
  index: number
  /** The block in which the transaction was added */
  block: number
  /** List an ERC20 asset */
  assetList?: ERC20AssetList | undefined
  /** De-list an ERC20 asset */
  assetDelist?: ERC20AssetDelist | undefined
  /** Deposit ERC20 asset */
  deposit?: ERC20Deposit | undefined
  /** Withdraw ERC20 asset */
  withdrawal?: ERC20Withdrawal | undefined
  /** Update an ERC20 asset */
  assetLimitsUpdated?: ERC20AssetLimitsUpdated | undefined
  /** Bridge operations has been stopped */
  bridgeStopped?: boolean | undefined
  /** Bridge operations has been resumed */
  bridgeResumed?: boolean | undefined
}

/** A new signer added to the ERC20 bridge */
export interface ERC20SignerAdded {
  /** The ethereum address of the new signer */
  newSigner: string
  /** The nonce create by the vega network used for this new signer */
  nonce: string
  /**
   * The time at which the block was produced
   * will be used to inform the core at what time
   * the stake was made unavailable.
   */
  blockTime: number
}

/** A signer removed from the ERC20 bridge */
export interface ERC20SignerRemoved {
  /** The ethereum address of the old signer */
  oldSigner: string
  /** The nonce create by the vega network used for this old signer */
  nonce: string
  /**
   * The time at which the block was produced
   * will be used to inform the core at what time
   * the stake was made unavailable.
   */
  blockTime: number
}

/** The threshold has been updated on the multisig control */
export interface ERC20ThresholdSet {
  /** The new threshold */
  newThreshold: number
  /** The nonce created by the Vega network */
  nonce: string
  /**
   * The time at which the block was produced
   * will be used to inform the core at what time
   * the stake was made unavailable.
   */
  blockTime: number
}

/** An event related to the ERC20 MultiSig */
export interface ERC20MultiSigEvent {
  /** Index of the log in the transaction */
  index: number
  /** The block in which the transaction was added */
  block: number
  /** Add a signer to the erc20 bridge */
  signerAdded?: ERC20SignerAdded | undefined
  /** Remove a signer from the erc20 bridge */
  signerRemoved?: ERC20SignerRemoved | undefined
  /** Threshold set */
  thresholdSet?: ERC20ThresholdSet | undefined
}

export interface StakingEvent {
  /** Index of the log in the transaction */
  index: number
  /** The block in which the transaction was added */
  block: number
  stakeDeposited?: StakeDeposited | undefined
  stakeRemoved?: StakeRemoved | undefined
  totalSupply?: StakeTotalSupply | undefined
}

export interface StakeDeposited {
  /** Ethereum Address of the user depositing stake (hex encode with 0x prefix) */
  ethereumAddress: string
  /** The public of the party receiving the stake deposit (hex encode) */
  vegaPublicKey: string
  /**
   * The amount deposited (base 10)
   * This field is an unsigned integer passed as a string and needs to be scaled using the asset's decimal places.
   */
  amount: string
  /**
   * The time at which the block was produced
   * will be used to inform the core at what time
   * the stake started to be available.
   */
  blockTime: number
}

export interface StakeRemoved {
  /** Ethereum address of the user removing stake (hex encode with 0x prefix) */
  ethereumAddress: string
  /** The public key of the party from which to remove stake (hex encode) */
  vegaPublicKey: string
  /**
   * The amount removed (base 10)
   * This field is an unsigned integer passed as a string and needs to be scaled using the asset decimal places for the staking token.
   */
  amount: string
  /**
   * The time at which the block was produced
   * will be used to inform the core at what time
   * the stake was made unavailable.
   */
  blockTime: number
}

export interface StakeTotalSupply {
  /** The address of the staking asset */
  tokenAddress: string
  /**
   * The total supply observed for the token
   * This field is an unsigned integer passed as a string and needs to be scaled using the asset decimal places for the staking token.
   */
  totalSupply: string
}