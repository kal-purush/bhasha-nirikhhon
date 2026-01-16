import { ActionProvider, CreateAction } from "@coinbase/agentkit";
import { z } from "zod";

export class ConnectWalletActionProvider extends ActionProvider {
  constructor() {
    super("connect_wallet", []);
  }

  supportsNetwork(): boolean {
    return true; // This action is network-independent
  }

  @CreateAction({
    name: "connect_metamask",
    description: `Connect to MetaMask wallet to interact with the blockchain.
    Thought: When a user needs to connect their wallet, I should:
    1. First check if MetaMask is installed
    2. Request wallet connection
    3. Return appropriate UI component
    Action: Return ConnectWallet component to handle the connection flow
    Observation: User will be prompted to connect their MetaMask wallet`,
    schema: z.null(),
  })
  async connectWallet(): Promise<{
    type: "component";
    component: string;
    props: Record<string, unknown>;
  }> {
    return {
      type: "component",
      component: "ConnectWallet",
      props: {},
    };
  }
}