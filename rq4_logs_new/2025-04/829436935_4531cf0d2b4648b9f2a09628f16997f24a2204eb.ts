import { ChildProcessWithoutNullStreams } from "child_process";
import { existsSync } from "fs";
import path from "path";
import {
    eboAbi,
    NotificationServiceFactory,
    ProposalId,
    ProposalStatus,
    ProtocolProviderV2,
} from "@ebo-agent/automated-dispute";
import { BlockNumberService } from "@ebo-agent/blocknumber";
import { Caip2ChainId, Logger, NotificationService } from "@ebo-agent/shared";
import { Address, getAbiItem, parseEther } from "viem";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";

import {
    AnvilTestEnvironment,
    ARBITRUM_SEPOLIA_ID,
    ARBITRUM_SEPOLIA_ID_HASH,
    BASE_GRT_AMOUNT,
    E2E_SCENARIO_SETUP_TIMEOUT,
    E2E_TEST_TIMEOUT,
    fundAccountWithGrt,
    GRT_HOLDER,
    killAgent,
    PROTOCOL_L2_CHAIN_ID,
    setupAnvilTestEnvironment,
    spawnAgent,
    stakeGrtWithProvision,
    waitForEvent,
} from "../../utils/e2e-scaffold/index.js";

interface RpcConfig {
    chainId: Caip2ChainId;
    urls: string[];
    transactionReceiptConfirmations: number;
    timeout: number;
    retryInterval: number;
}

describe.sequential("max proposals per epoch and chain", () => {
    const tmpConfigDir = path.join(__dirname, "/tmp");
    const tmpConfigFile = path.join(tmpConfigDir, "/config.yml");

    let agent: ChildProcessWithoutNullStreams | undefined;
    let testEnv: AnvilTestEnvironment;

    let protocolProvider: ProtocolProviderV2; // Agent's provider
    let adversarialProvider: ProtocolProviderV2; // Adversarial actor's provider
    let blockNumberService: BlockNumberService;

    const notifier: NotificationService = NotificationServiceFactory.create(Logger.getInstance());
    const logger = Logger.getInstance();

    let baseProviderConfig: {
        l1: RpcConfig;
        l2: RpcConfig;
    };

    let contractAddresses: {
        epochManager: Address;
        ebo: Address;
    };
    beforeAll(async () => {
        testEnv = await setupAnvilTestEnvironment(__dirname);
    }, E2E_SCENARIO_SETUP_TIMEOUT);

    afterAll(async () => {
        await testEnv.cleanup();
    });

    beforeEach(async () => {
        blockNumberService = new BlockNumberService(
            new Map<Caip2ChainId, string[]>([[PROTOCOL_L2_CHAIN_ID, [testEnv.localUrl]]]),
            {
                baseUrl: new URL("http://not.needed/"),
                bearerToken: "not.needed",
                bearerTokenExpirationWindow: 1000,
                servicePaths: {
                    block: "/block",
                    blockByTime: "/blockByTime",
                },
            },
            logger,
            notifier,
        );

        baseProviderConfig = {
            l1: {
                chainId: PROTOCOL_L2_CHAIN_ID as Caip2ChainId,
                urls: [testEnv.localUrl],
                transactionReceiptConfirmations: 1,
                timeout: 1_000,
                retryInterval: 500,
            },
            l2: {
                chainId: PROTOCOL_L2_CHAIN_ID as Caip2ChainId,
                urls: [testEnv.localUrl],
                transactionReceiptConfirmations: 1,
                timeout: 1_000,
                retryInterval: 500,
            },
        };

        contractAddresses = {
            epochManager: testEnv.protocolContracts.epochManager,
            ebo: testEnv.protocolContracts.eboProxy,
        };
    });

    afterEach(async () => {
        await testEnv.anvilClient.revert({ id: testEnv.snapshotId });

        if (existsSync(tmpConfigFile) && agent) {
            await killAgent({
                process: agent,
                configPath: tmpConfigFile,
            });
            agent = undefined; // Reset agent variable
        }
    });

    it(
        "agent should not create more than 5 proposals for the same epoch and chain",
        { timeout: E2E_TEST_TIMEOUT },
        async () => {
            const { anvilClient, protocolContracts, withoutStakeAccounts } = testEnv;
            const MAX_PROPOSALS = 5;

            const agentAccount = withoutStakeAccounts[0];
            const adversarialAccount = withoutStakeAccounts[1];

            // Check if accounts and keys exist
            if (
                !withoutStakeAccounts ||
                withoutStakeAccounts.length < 2 ||
                !agentAccount?.privateKey ||
                !adversarialAccount?.privateKey
            ) {
                throw new Error("Required test accounts not found or missing private keys.");
            }

            const grtFundAmount = parseEther((BASE_GRT_AMOUNT * 3n).toString());
            await fundAccountWithGrt(agentAccount.account, anvilClient, {
                holderAddress: GRT_HOLDER,
                contractAddress: protocolContracts.grt,
                fundAmount: grtFundAmount,
            });
            await fundAccountWithGrt(adversarialAccount.account, anvilClient, {
                holderAddress: GRT_HOLDER,
                contractAddress: protocolContracts.grt,
                fundAmount: grtFundAmount,
            });

            await stakeGrtWithProvision(
                [agentAccount.account, adversarialAccount.account],
                {
                    grt: protocolContracts.grt,
                    horizonStaking: protocolContracts.horizonStaking,
                    verifier: protocolContracts.eboProxy,
                },
                grtFundAmount,
                anvilClient,
            );

            // Provider for the agent
            protocolProvider = new ProtocolProviderV2(
                baseProviderConfig,
                contractAddresses,
                agentAccount.privateKey,
                logger,
                blockNumberService,
            );

            // Provider for the adversarial actor
            adversarialProvider = new ProtocolProviderV2(
                baseProviderConfig,
                contractAddresses,
                adversarialAccount.privateKey,
                logger,
                blockNumberService,
            );

            const initBlock = await anvilClient.getBlockNumber();
            const currentEpoch = await protocolProvider.read.getCurrentEpoch();

            // Spawn the agent with default max proposals per epoch and chain (5)
            agent = spawnAgent({
                configPath: tmpConfigFile,
                config: {
                    protocolProvider: {
                        contracts: {
                            ebo: protocolContracts.eboProxy,
                            epochManager: protocolContracts.epochManager,
                        },
                        rpcsConfig: {
                            l1: {
                                chainId: PROTOCOL_L2_CHAIN_ID,
                                transactionReceiptConfirmations: 1,
                                timeout: 1_000,
                                retryInterval: 500,
                            },
                            l2: {
                                chainId: PROTOCOL_L2_CHAIN_ID,
                                transactionReceiptConfirmations: 1,
                                timeout: 1_000,
                                retryInterval: 500,
                            },
                        },
                    },
                    blockNumberService: {
                        blockmetaConfig: {
                            baseUrl: new URL("http://not.needed/"),
                            bearerTokenExpirationWindow: 1000,
                            servicePaths: {
                                block: "/block",
                                blockByTime: "/blockByTime",
                            },
                        },
                    },
                    processor: {
                        // Check frequently to react quickly in the test
                        msBetweenChecks: 1000,
                    },
                },
                env: {
                    PROTOCOL_PROVIDER_PRIVATE_KEY: agentAccount.privateKey,
                    PROTOCOL_PROVIDER_L1_RPC_URLS: [testEnv.localUrl],
                    PROTOCOL_PROVIDER_L2_RPC_URLS: [testEnv.localUrl],
                    BLOCK_NUMBER_BLOCKMETA_TOKEN: "not.needed",
                    BLOCK_NUMBER_RPC_URLS_MAP: new Map<Caip2ChainId, string[]>([
                        [PROTOCOL_L2_CHAIN_ID, [testEnv.localUrl]],
                    ]),
                },
            });

            let agentOutput = "";
            agent.stdout.on("data", (data) => {
                agentOutput += data.toString();
            });

            // Array to store the created proposal IDs
            const proposalIds: ProposalId[] = [];
            let fromBlock = initBlock;

            // 1. Loop to create and dispute MAX_PROPOSALS proposals
            for (let i = 0; i < MAX_PROPOSALS; i++) {
                // Wait for the agent to create a proposal
                const proposalCreatedEvent = await waitForEvent({
                    client: anvilClient,
                    filter: {
                        address: protocolContracts.eboProxy,
                        fromBlock,
                        event: getAbiItem({ abi: eboAbi, name: "ProposalCreated" }),
                        strict: true,
                    },
                    matcher: (log) => {
                        return (
                            log.args._serviceProvider === agentAccount.account.address &&
                            log.args._epoch === currentEpoch.number &&
                            log.args._chainId === ARBITRUM_SEPOLIA_ID_HASH
                        );
                    },
                    pollingIntervalMs: 100,
                    blockTimeout: fromBlock + 1000n,
                });

                expect(proposalCreatedEvent).toBeDefined();
                const proposalId = proposalCreatedEvent.args._proposalId as ProposalId;
                proposalIds.push(proposalId);
                const activeProposal = await adversarialProvider.read.getActiveProposal(
                    currentEpoch.number,
                    ARBITRUM_SEPOLIA_ID,
                );

                // Have the adversarial actor dispute the proposal to ensure it's removed from active proposals
                await adversarialProvider.write.dispute(
                    activeProposal!.id,
                    adversarialAccount.account.address,
                );

                console.log("disputed proposal", activeProposal!.id);

                // Mine some blocks to ensure the events are processed
                await anvilClient.mine({ blocks: 2 });

                // Update the fromBlock for the next proposal search
                fromBlock = proposalCreatedEvent.blockNumber + 1n;
            }

            expect(proposalIds.length).toEqual(MAX_PROPOSALS);

            // 2. Wait some time to give the agent a chance to potentially create a 6th proposal
            // The key validation is that no additional proposals should be created
            await new Promise((resolve) => setTimeout(resolve, 3000)); // Wait 3 seconds
            await anvilClient.mine({ blocks: 10 }); // Mine some blocks to trigger agent sync

            // 3. Try to detect if a 6th proposal was created
            const activeProposal = await adversarialProvider.read.getActiveProposal(
                currentEpoch.number,
                ARBITRUM_SEPOLIA_ID,
            );
            expect(activeProposal).toBeUndefined();

            // 4. Check agent logs for the expected message
            await vi.waitFor(
                () =>
                    expect(agentOutput).toContain(
                        `Max proposals per epoch and chain reached for epoch ${currentEpoch.number} and chain ${PROTOCOL_L2_CHAIN_ID}`,
                    ),
                { timeout: 5000 },
            );

            // 5. Verify all created proposals exist and have the correct status
            for (const proposalId of proposalIds) {
                const proposal = await protocolProvider.read.getProposalById(proposalId);
                expect(proposal).toBeDefined();
                expect(proposal?.status).toEqual(ProposalStatus.Disputed);
            }

            // Check for any critical errors in logs
            expect(agentOutput).not.toContain(`Critical error`);
        },
    );
});