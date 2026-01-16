import { ActionProvider, CreateAction } from "@coinbase/agentkit";
import { z } from "zod";
import { createPublicClient, http, parseAbi, Address } from "viem";
import { base } from "viem/chains";

// Minimal ERC20 ABI for the functions we need
const CONTRACT_ABI = parseAbi([
  "function balanceOf(address account) view returns (uint256)",
  "function name() view returns (string)",
  "function decimals() view returns (uint8)",
  "function totalSupply() view returns (uint256)",
]);

const CONTRACT_ADDRESS = "0xce827d203dc317cb8823098dcbe88fd3ae447482";
const TOKEN_PRICE_USD = 10; // Fixed price as specified

export class ZeroXEquityActionProvider extends ActionProvider {
  constructor() {
    super(
      "0xequity is a RWA tokenization platform which allows investor to invest with as low as 10$ and earn 10% rental yield annually. its a way for users to earn some extra buck without investing huge capital",
      []
    );
  }

  supportsNetwork(): boolean {
    return true; // This action works on Base mainnet
  }

  @CreateAction({
    name: "get_invested_properties",
    description: `Get information about invested properties from the Base mainnet smart contract.
    Thought process:
    1. Check if wallet address is provided
    2. If no address, explain that a wallet address is required and stop
    3. If address provided:
       - Query token name, balance, total supply, and decimals
       - Calculate ownership percentage
       - Format and return detailed investment information
    
    Example valid responses:
    - Without address: "Please provide a wallet address to check property investments. Usage: 'Check investments for 0x...''"
    - With invalid address: "Please provide a valid Ethereum wallet address starting with '0x'"
    - With valid address: Returns full investment details
    
    Important: This action requires a valid Ethereum wallet address to proceed. Never use already given wallet address. Always ask for a new one.`,
    schema: z.object({
      address: z
        .string()
        .optional()
        .describe("The wallet address to check property investments for"),
    }),
  })
  async getInvestedProperties(params: { address?: string }): Promise<string> {
    if (!params.address) {
      return `Please provide a wallet address to check property investments. 
Usage: "Check investments for 0x..."`;
    }

    if (!params.address.startsWith("0x")) {
      return `Please provide a valid Ethereum wallet address starting with '0x'`;
    }

    try {
      const client = createPublicClient({
        chain: base,
        transport: http(),
      });

      // Ensure address is properly formatted as hex
      const formattedAddress = params.address as Address;

      // Get token data including total supply
      const [name, balance, decimals, totalSupply] = await Promise.all([
        client.readContract({
          address: CONTRACT_ADDRESS,
          abi: CONTRACT_ABI,
          functionName: "name",
        }),
        client.readContract({
          address: CONTRACT_ADDRESS,
          abi: CONTRACT_ABI,
          functionName: "balanceOf",
          args: [formattedAddress],
        }),
        client.readContract({
          address: CONTRACT_ADDRESS,
          abi: CONTRACT_ABI,
          functionName: "decimals",
        }),
        client.readContract({
          address: CONTRACT_ADDRESS,
          abi: CONTRACT_ABI,
          functionName: "totalSupply",
        }),
      ]);

      // Convert balance and total supply to human readable format
      const divisor = Math.pow(10, Number(decimals));
      const tokenBalance = Number(balance) / divisor;
      const totalTokenSupply = Number(totalSupply) / divisor;
      const ownershipPercentage = (tokenBalance / totalTokenSupply) * 100;
      const totalValueUSD = tokenBalance * TOKEN_PRICE_USD;

      return `Investment Portfolio for ${params.address}:
Property Token: ${name}
Tokens Owned: ${tokenBalance.toFixed(4)} tokens
Total Supply: ${totalTokenSupply.toFixed(4)} tokens
Ownership Percentage: ${ownershipPercentage.toFixed(2)}%
Total Value: $${totalValueUSD.toFixed(2)} (at $10 per token)`;
    } catch (error) {
      if (error instanceof Error) {
        throw new Error(
          `Failed to fetch property investments: ${error.message}`
        );
      }
      throw new Error("Failed to fetch property investments: Unknown error");
    }
  }
}