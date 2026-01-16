import { WalletProvider, Network } from "@coinbase/agentkit";
import { TransactionRequest as EthersTransactionRequest } from "ethers";

/**
 * MetaMaskWalletProvider
 * 
 * A backend-only implementation of WalletProvider for AgentKit
 * that works with a user's connected MetaMask wallet. This provider does not
 * execute transactions or sign messages directly - it only prepares payloads
 * that will be sent to the frontend for the user to approve and execute.
 */
export class MetaMaskWalletProvider extends WalletProvider {
  private readonly currentSessionUserAddress: string | null;
  private readonly currentSessionChainId: number | null;

  /**
   * Creates a new instance of the MetaMaskWalletProvider
   * 
   * @param address The wallet address from the user's current session (null if no wallet is connected)
   * @param chainId The chain ID from the user's current session (null if no wallet is connected)
   */
  constructor(address?: string | null, chainId?: number | null) {
    super();
    this.currentSessionUserAddress = address || null;
    this.currentSessionChainId = chainId || null;
  }

  /**
   * Get the name of the wallet provider
   */
  getName(): string {
    return 'MetaMask';
  }

  /**
   * Get the network information from the current session
   */
  getNetwork(): Network {
    return {
      networkId: this.currentSessionChainId ? this.currentSessionChainId.toString() : 'unknown',
      protocolFamily: 'evm'
    };
  }

  /**
   * Get the wallet address from the current user's session
   * @returns The wallet address or empty string if no wallet is connected
   */
  getAddress(): string {
    return this.currentSessionUserAddress || '';
  }

  /**
   * Get the chain ID from the current user's session
   * @returns The chain ID or null if no wallet is connected
   */
  async getChainId(): Promise<number | null> {
    return this.currentSessionChainId;
  }

  /**
   * Get the wallet balance
   * This method is not available in the backend-only implementation
   * @throws Error indicating that a connected wallet is required
   */
  async getBalance(): Promise<bigint> {
    if (!this.currentSessionUserAddress) {
      throw new Error('WalletNotConnected: A connected wallet is required to get the balance.');
    }
    
    // In a more complete implementation, we might use a generic ethers.JsonRpcProvider
    // to fetch the balance of the connected wallet, but for now we'll throw an error
    throw new Error('NotImplemented: Balance checking is not supported in the backend MetaMask provider.');
  }

  /**
   * Prepares a transaction payload for later execution by the frontend MetaMask wallet.
   * This method is backend-only and DOES NOT interact with frontend APIs.
   * 
   * @param txDetails The transaction details
   * @returns The prepared transaction payload
   * @throws Error if wallet is not connected
   */
  async prepareTransactionPayload(txDetails: EthersTransactionRequest): Promise<EthersTransactionRequest> {
    if (!this.currentSessionUserAddress) {
      throw new Error('WalletNotConnected: A connected wallet is required to prepare a transaction payload.');
    }

    // Ensure the 'from' field correctly reflects the connected user's address
    const payload: EthersTransactionRequest = { 
      ...txDetails,
      from: this.currentSessionUserAddress 
    };

    // You might add further validation or default gas settings here if appropriate
    return payload;
  }

  /**
   * Native token transfer (required by WalletProvider interface)
   * @param to Recipient address
   * @param value Amount to send as a string
   * @returns Transaction hash as a string
   */
  async nativeTransfer(to: string, value: string): Promise<string> {
    if (!this.currentSessionUserAddress) {
      throw new Error('WalletNotConnected: A connected wallet is required to prepare a transfer.');
    }
    
    const tx = {
      to,
      value,
      from: this.currentSessionUserAddress,
    };
    
    // Return the prepared transaction for approval in the frontend
    // Since we can't actually execute the transaction in the backend, we'll throw an error
    // This matches the behavior of sendTransaction
    throw new Error('InvalidBackendOperation: Transaction submission via MetaMask must be performed on the frontend. ' +
      'This provider prepares payloads only.');
  }

  /**
   * Prepares a message for signing by the frontend MetaMask wallet.
   * This method is backend-only and DOES NOT interact with frontend APIs.
   * 
   * @param message The message to sign
   * @returns The prepared message
   * @throws Error if wallet is not connected
   */
  async prepareMessageSignaturePayload(message: string | Uint8Array): Promise<string | Uint8Array> {
    if (!this.currentSessionUserAddress) {
      throw new Error('WalletNotConnected: A connected wallet is required to prepare a message signature payload.');
    }
    
    return message;
  }

  /**
   * Sign a message - NOT IMPLEMENTED for the backend-only provider
   * This method exists only to satisfy the WalletProvider interface
   * 
   * @throws Error indicating this operation must be performed on the frontend
   */
  async signMessage(message: string | Uint8Array): Promise<string> {
    console.error("CRITICAL: MetaMaskWalletProvider backend 'signMessage' was invoked. " +
      "This indicates a misconfiguration or a tool not designed for the HITL MetaMask flow. " +
      "All signing must occur on the frontend after user approval.");
      
    throw new Error('InvalidBackendOperation: Signing via MetaMask must be performed on the frontend. ' +
      'This provider prepares payloads only.');
  }

  /**
   * Send a transaction - NOT IMPLEMENTED for the backend-only provider
   * This method exists only to satisfy the WalletProvider interface
   * 
   * @throws Error indicating this operation must be performed on the frontend
   */
  async sendTransaction(transaction: EthersTransactionRequest): Promise<any> {
    console.error("CRITICAL: MetaMaskWalletProvider backend 'sendTransaction' was invoked. " +
      "This is incorrect for the HITL MetaMask flow. " +
      "All transaction submissions must occur on the frontend after user approval.");
      
    throw new Error('InvalidBackendOperation: Transaction submission via MetaMask must be performed on the frontend. ' +
      'This provider prepares payloads only.');
  }
}