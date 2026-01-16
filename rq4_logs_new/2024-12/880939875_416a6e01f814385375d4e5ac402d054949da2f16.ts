export const WALLET_MESSAGE_VERSIONS = {
  "0.1": {
    template: `Welcome to Mina Ecosystem Funding! 👋

By signing this message with your wallet ({{walletAddress}}), you confirm and agree that:

✨ You want to create an account or login to your existing account
💾 We'll store this signature and your wallet address in our database
📊 We'll collect and store data about your platform activities, including, but not limited to:
• Your profile information
• Proposals you create
• Comments you make
• Other platform interactions

This signature serves as your conscent and will be recorded at {{timestamp}} UTC.

Don't want this? No problem - just don't sign the message! You can always create an account using alternative methods.

If something is not clear, or you have any questions - reach out to our team.

Signature Version: 0.1`,
    generateMessage: (walletAddress: string, timestamp: string) => {
      return WALLET_MESSAGE_VERSIONS["0.1"].template
        .replace("{{walletAddress}}", walletAddress)
        .replace("{{timestamp}}", timestamp);
    }
  }
} as const;

export const LATEST_WALLET_MESSAGE_VERSION = "0.1";

export type WalletMessageVersion = keyof typeof WALLET_MESSAGE_VERSIONS; 