import { ActionProvider, CreateAction } from "@coinbase/agentkit";
import { z } from "zod";

export class XWalletActionProvider extends ActionProvider {
  constructor() {
    super("xwallet", []);
  }

  supportsNetwork(): boolean {
    return true; // This action is network-independent
  }

  @CreateAction({
    name: "connect_xwallet",
    description: `Connect to xWallet using passkeys to interact with the blockchain.
    Thought: When a user needs to connect their xWallet, I should:
    1. Show email input form
    2. Submit email to endpoint
    3. Handle passkey signing flow
    Action: Return XWallet component to handle the connection flow
    Observation: User will be prompted to connect their xWallet`,
    schema: z.null(),
  })
  async connectWallet(): Promise<{
    type: "component";
    component: string;
    props: Record<string, unknown>;
  }> {
    return {
      type: "component",  
      component: "XWallet",
      props: {},
    };
  }
}