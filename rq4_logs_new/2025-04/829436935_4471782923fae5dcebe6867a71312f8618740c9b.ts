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
import { getAbiItem } from "viem";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";

import {
    AnvilTestEnvironment,
    ARBITRUM_SEPOLIA_ID,
    ARBITRUM_SEPOLIA_ID_HASH,
    E2E_SCENARIO_SETUP_TIMEOUT,
    E2E_TEST_TIMEOUT,
    killAgent,
    PROTOCOL_L2_CHAIN_ID,
    setupAnvilTestEnvironment,
    spawnAgent,
    waitForEvent,
} from "../../utils/e2e-scaffold/index.js";

describe.sequential("dispute wrong proposal and win", () => {
    const tmpConfigDir = path.join(__dirname, "/tmp");
    const tmpConfigFile = path.join(tmpConfigDir, "/config.yml");

    let agent: ChildProcessWithoutNullStreams | undefined;
    let testEnv: AnvilTestEnvironment;

    let protocolProvider: ProtocolProviderV2;
    let blockNumberService: BlockNumberService;

    const notifier: NotificationService = NotificationServiceFactory.create(Logger.getInstance());
    const logger = Logger.getInstance();

    beforeAll(async () => {
        // Setup the test environment with dynamic port allocation
        testEnv = await setupAnvilTestEnvironment(__dirname);
    }, E2E_SCENARIO_SETUP_TIMEOUT);

    afterAll(async () => {
        // Clean up the test environment
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
    }, E2E_SCENARIO_SETUP_TIMEOUT);

    afterEach(async () => {
        await testEnv.anvilClient.revert({ id: testEnv.snapshotId });

        if (existsSync(tmpConfigFile) && agent) {
            await killAgent({
                process: agent,
                configPath: tmpConfigFile,
            });
        }
    });

    it(
        "agent disputes wrong proposal, wins the dispute, and creates a correct proposal",
        { timeout: E2E_TEST_TIMEOUT },
        async () => {
            const {
                anvilClient,
                protocolContracts,
                withStakeAccounts,
                withoutStakeAccounts,
                localUrl,
            } = testEnv;

            const agentAccount = withStakeAccounts[0];
            const adversarialAccount = withStakeAccounts[1];
            const e2eAccount = withoutStakeAccounts[0];
            if (
                !withStakeAccounts ||
                withStakeAccounts.length < 2 ||
                agentAccount?.privateKey === undefined ||
                adversarialAccount?.privateKey === undefined ||
                e2eAccount?.privateKey === undefined
            ) {
                throw new Error("Not enough accounts found");
            }

            protocolProvider = new ProtocolProviderV2(
                {
                    l1: {
                        chainId: PROTOCOL_L2_CHAIN_ID,
                        urls: [testEnv.localUrl],
                        transactionReceiptConfirmations: 1,
                        timeout: 1_000,
                        retryInterval: 500,
                    },
                    l2: {
                        chainId: PROTOCOL_L2_CHAIN_ID,
                        urls: [testEnv.localUrl],
                        transactionReceiptConfirmations: 1,
                        timeout: 1_000,
                        retryInterval: 500,
                    },
                },
                {
                    epochManager: testEnv.protocolContracts.epochManager,
                    ebo: testEnv.protocolContracts.eboProxy,
                },
                e2eAccount.privateKey,
                logger,
                blockNumberService,
            );

            const currentEpoch = await protocolProvider.getCurrentEpoch();
            const initBlock = await anvilClient.getBlockNumber();

            // Create a separate protocol provider for the "adversarial" actor using a different account
            const adversarialProvider = new ProtocolProviderV2(
                {
                    l1: {
                        chainId: PROTOCOL_L2_CHAIN_ID,
                        urls: [testEnv.localUrl],
                        transactionReceiptConfirmations: 1,
                        timeout: 1_000,
                        retryInterval: 500,
                    },
                    l2: {
                        chainId: PROTOCOL_L2_CHAIN_ID,
                        urls: [testEnv.localUrl],
                        transactionReceiptConfirmations: 1,
                        timeout: 1_000,
                        retryInterval: 500,
                    },
                },
                {
                    epochManager: testEnv.protocolContracts.epochManager,
                    ebo: testEnv.protocolContracts.eboProxy,
                },
                adversarialAccount.privateKey,
                logger,
                blockNumberService,
            );

            // Get the correct block number
            const correctBlockNumber = await blockNumberService.getEpochBlockNumber(
                currentEpoch.startTimestamp,
                PROTOCOL_L2_CHAIN_ID,
            );

            // Create a proposal with an INCORRECT block number (+1)
            const wrongBlockNumber = correctBlockNumber + 1n;

            // Create adversarial proposal with wrong block number
            await adversarialProvider.propose(
                currentEpoch.number,
                ARBITRUM_SEPOLIA_ID,
                wrongBlockNumber,
                adversarialAccount.account.address,
            );
            await anvilClient.mine({ blocks: 1 });

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
                        msBetweenChecks: 1000,
                    },
                },
                env: {
                    PROTOCOL_PROVIDER_PRIVATE_KEY: agentAccount.privateKey,
                    PROTOCOL_PROVIDER_L1_RPC_URLS: [localUrl],
                    PROTOCOL_PROVIDER_L2_RPC_URLS: [localUrl],
                    BLOCK_NUMBER_BLOCKMETA_TOKEN: "not.needed",
                    BLOCK_NUMBER_RPC_URLS_MAP: new Map<Caip2ChainId, string[]>([
                        [PROTOCOL_L2_CHAIN_ID, [localUrl]],
                    ]),
                },
            });

            let agentOutput = ""; // Variable to capture agent's stdout
            // Capture agent's stdout and stderr
            agent.stdout.on("data", (data) => {
                agentOutput += data.toString();
            });

            // Wait for the ProposalCreated event
            const proposalCreatedAbi = getAbiItem({ abi: eboAbi, name: "ProposalCreated" });
            let wrongProposalId: bigint | undefined;

            await waitForEvent({
                client: anvilClient,
                filter: {
                    address: protocolContracts.eboProxy,
                    fromBlock: initBlock,
                    event: proposalCreatedAbi,
                    strict: true,
                },
                matcher: (log) => {
                    const chainId = log.args._chainId;
                    const epoch = log.args._epoch;
                    const blockNumber = log.args._blockNumber;

                    if (
                        chainId !== ARBITRUM_SEPOLIA_ID_HASH ||
                        epoch !== currentEpoch.number ||
                        blockNumber !== wrongBlockNumber
                    ) {
                        return false;
                    }

                    wrongProposalId = log.args._proposalId;
                    return true;
                },
                pollingIntervalMs: 100,
                blockTimeout: initBlock + 1000n,
            });

            // Wait for the agent to dispute the wrong proposal
            const proposalDisputedAbi = getAbiItem({ abi: eboAbi, name: "ProposalDisputed" });

            const proposalDisputedEvent = await waitForEvent({
                client: anvilClient,
                filter: {
                    address: protocolContracts.eboProxy,
                    fromBlock: initBlock,
                    event: proposalDisputedAbi,
                    strict: true,
                },
                matcher: (log) => {
                    return log.args._proposalId === wrongProposalId;
                },
                pollingIntervalMs: 100,
                blockTimeout: initBlock + 1000n,
            });

            expect(proposalDisputedEvent).toBeDefined();

            // Wait for the agent's pledge for its dispute with increased timeout
            const pledgeForDisputeAbi = getAbiItem({ abi: eboAbi, name: "PledgedForDispute" });

            const pledgeForDisputeEvent = await waitForEvent({
                client: anvilClient,
                filter: {
                    address: protocolContracts.eboProxy,
                    fromBlock: initBlock,
                    event: pledgeForDisputeAbi,
                    strict: true,
                },
                matcher: (log) => {
                    return (
                        log.args._proposalId === wrongProposalId &&
                        log.args._pledger === agentAccount.account.address
                    );
                },
                pollingIntervalMs: 200,
                blockTimeout: initBlock + 2000n,
            });

            expect(pledgeForDisputeEvent).toBeDefined();

            await anvilClient.mine({ blocks: 20 });

            // The adversarial actor pledges against the dispute
            await adversarialProvider.pledgeAgainstDispute(
                wrongProposalId as ProposalId,
                adversarialAccount.account.address,
            );

            // Mine blocks to ensure the transaction is processed
            await anvilClient.mine({ blocks: 1 });

            const secondPledgeForDisputeEvent = await waitForEvent({
                client: anvilClient,
                filter: {
                    address: protocolContracts.eboProxy,
                    fromBlock: pledgeForDisputeEvent.blockNumber + 1n,
                    event: pledgeForDisputeAbi,
                    strict: true,
                },
                matcher: (log) => {
                    return (
                        log.args._proposalId === wrongProposalId &&
                        log.args._pledger === agentAccount.account.address
                    );
                },
                pollingIntervalMs: 100,
                blockTimeout: pledgeForDisputeEvent.blockNumber + 1000n,
            });

            expect(secondPledgeForDisputeEvent).toBeDefined();

            // Get the dispute and snapshot parameters
            const dispute = await protocolProvider.getDisputeByProposalId(
                wrongProposalId as ProposalId,
            );
            expect(dispute).toBeDefined();

            const proposal = await protocolProvider.getProposalById(wrongProposalId as ProposalId);
            expect(proposal).toBeDefined();

            const snapshotParameters = await protocolProvider.getSnapshotParameters(
                proposal!.parametersSnapshotId,
            );
            expect(snapshotParameters).toBeDefined();

            // Advance time past the pledge window to allow dispute resolution
            // Increase by 20% to ensure we're well past the window
            await anvilClient.increaseTime({
                seconds: Math.floor(
                    (Number(snapshotParameters!.pledgeWindow) +
                        Number(snapshotParameters!.pledgeRefreshWindow)) *
                        1.2,
                ),
            });
            await anvilClient.mine({ blocks: 1 });

            // Wait for the agent to resolve the dispute permissionlessly with increased timeout
            const disputeResolvedAbi = getAbiItem({ abi: eboAbi, name: "DisputeResolved" });

            const disputeResolvedEvent = await waitForEvent({
                client: anvilClient,
                filter: {
                    address: protocolContracts.eboProxy,
                    fromBlock: secondPledgeForDisputeEvent.blockNumber,
                    event: disputeResolvedAbi,
                    strict: true,
                },
                matcher: (log) => {
                    return log.args._proposalId === wrongProposalId;
                },
                pollingIntervalMs: 200,
                blockTimeout: secondPledgeForDisputeEvent.blockNumber + 2000n,
            });

            expect(disputeResolvedEvent).toBeDefined();
            expect(disputeResolvedEvent?.args._resolutionStatus).toEqual(
                ProposalStatus.DisputerWon,
            );

            // Verify the dispute was resolved with Disputer Won
            const resolvedProposal = await protocolProvider.getProposalById(
                wrongProposalId as ProposalId,
            );
            expect(resolvedProposal?.status).toEqual(ProposalStatus.DisputerWon);

            // the agent should have created a new proposal with the correct block number
            const correctProposalEvent = await waitForEvent({
                client: anvilClient,
                filter: {
                    address: protocolContracts.eboProxy,
                    fromBlock: proposalDisputedEvent.blockNumber,
                    event: proposalCreatedAbi,
                    strict: true,
                },
                matcher: (log) => {
                    const chainId = log.args._chainId;
                    const epoch = log.args._epoch;
                    const blockNumber = log.args._blockNumber;

                    return (
                        chainId === ARBITRUM_SEPOLIA_ID_HASH &&
                        epoch === currentEpoch.number &&
                        blockNumber === correctBlockNumber &&
                        log.args._serviceProvider === agentAccount.account.address
                    );
                },
                pollingIntervalMs: 300,
                blockTimeout: proposalDisputedEvent.blockNumber + 5000n,
            });

            expect(correctProposalEvent).toBeDefined();
            expect(correctProposalEvent?.args._blockNumber).toBe(correctBlockNumber);

            expect(correctProposalEvent?.args._proposalId).not.toBe(wrongProposalId);

            await anvilClient.increaseTime({
                seconds: Number(snapshotParameters?.disputeWindow),
            });

            const proposalFinalizedAbi = getAbiItem({
                abi: eboAbi,
                name: "ProposalFinalized",
            });

            const proposalFinalizedEvent = await waitForEvent({
                client: anvilClient,
                filter: {
                    address: protocolContracts.eboProxy,
                    fromBlock: correctProposalEvent.blockNumber,
                    event: proposalFinalizedAbi,
                    strict: true,
                },
                matcher: (log) => {
                    return (
                        log.args._proposalId === correctProposalEvent?.args._proposalId &&
                        log.args._chainId === ARBITRUM_SEPOLIA_ID_HASH &&
                        log.args._epoch === currentEpoch.number
                    );
                },
                pollingIntervalMs: 100,
                blockTimeout: correctProposalEvent.blockNumber + 1000n,
            });

            expect(proposalFinalizedEvent).toBeDefined();

            await vi.waitFor(() => expect(agentOutput).not.toContain(`Critical error`), {
                timeout: 5000,
            });

            // Increase timeouts for verification checks
            await vi.waitFor(
                () =>
                    expect(agentOutput).toContain(
                        `Proposal ${wrongProposalId} disputed successfully`,
                    ),
                { timeout: 5000 },
            );

            await vi.waitFor(
                () =>
                    expect(agentOutput).toContain(
                        `Pledged 'for' dispute on proposal ${wrongProposalId}`,
                    ),
                { timeout: 5000 },
            );

            await vi.waitFor(
                () =>
                    expect(agentOutput).toContain(
                        `Proposal ${correctProposalEvent?.args._proposalId} has been finalized.`,
                    ),
                {
                    timeout: 5000,
                },
            );
        },
    );
});