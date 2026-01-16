import { isNativeError } from "util/types";
import { BlockNumberService } from "@ebo-agent/blocknumber";
import {
    Caip2ChainId,
    Caip2Utils,
    ILogger,
    NotificationService,
    stringify,
    UnixTimestamp,
} from "@ebo-agent/shared";
import { Block } from "viem";

import {
    CustomContractErrorV2,
    PastEventEnqueueError,
    ProcessorAlreadyStarted,
    TransactionExecutionError,
} from "../exceptions/index.js";
import { isProposalCreatedEvent } from "../guardsV2.js";
import { IEboParametersRegistry } from "../interfaces/index.js";
import { ProtocolProviderV2 } from "../providers/index.js";
import { EboActorsManagerV2 } from "../services/index.js";
import { ActorProposal, EboEventV2, Epoch, ProposalId, ProposalStatus } from "../types/index.js";

const DEFAULT_MS_BETWEEN_CHECKS = 10 * 60 * 1000; // 10 minutes
const MAX_PROPOSALS_PER_EPOCH_AND_CHAIN = 5;

type EboEventStream = EboEventV2[];
type EpochChainKey = `${Epoch["number"]}-${Caip2ChainId}`;

/**
 * EboProcessorV2 handles the synchronization of events from the blockchain and manages the lifecycle of EboActorsV2.
 *
 * It polls for events, creates new proposals, and manages actors for handling proposals throughout their lifecycle.
 */
export class EboProcessorV2 {
    private eventsInterval?: NodeJS.Timeout;
    private lastCheckedBlock?: bigint;
    private handledChainIdsPerEpoch: Map<Epoch["number"], Set<Caip2ChainId>> = new Map();
    private counterProposalsCreatedPerEpochAndChain: Map<EpochChainKey, number> = new Map();

    constructor(
        private readonly protocolProvider: ProtocolProviderV2,
        private readonly blockNumberService: BlockNumberService,
        private readonly actorsManager: EboActorsManagerV2,
        private readonly parametersRegistry: IEboParametersRegistry,
        private readonly logger: ILogger,
        private readonly notifier: NotificationService,
        private readonly maxProposalsPerEpochAndChain: number = MAX_PROPOSALS_PER_EPOCH_AND_CHAIN,
    ) {}

    /**
     * Start syncing blocks and events
     *
     * @param msBetweenChecks milliseconds between each sync
     */
    public async start(msBetweenChecks: number = DEFAULT_MS_BETWEEN_CHECKS) {
        if (this.eventsInterval) throw new ProcessorAlreadyStarted();

        await this.sync(); // Bootstrapping

        this.eventsInterval = setInterval(async () => {
            try {
                await this.sync();
            } catch (err) {
                this.logger.error(`Unhandled error during the event loop: ${err}`);

                await this.notifier.sendError("Unhandled error during the event loop", {}, err);

                clearInterval(this.eventsInterval);

                throw err;
            }
        }, msBetweenChecks);
    }

    /** Sync new blocks and their events with their corresponding actors. */
    private async sync() {
        try {
            const currentEpoch = await this.getCurrentEpoch();

            if (!this.lastCheckedBlock) {
                // We want to emulate the previous epoch being fully checked
                this.lastCheckedBlock = currentEpoch.firstBlockNumber - 1n;
            }

            const lastBlock = await this.getLastFinalizedBlock();

            // Events will sync starting from the block after the last checked one,
            // making the block interval exclusive of its lower bound:
            //  (last checked block, last block]
            const events = await this.getEvents(this.lastCheckedBlock + 1n, lastBlock.number);

            const eventsByProposalId = this.groupEventsByProposal(events);
            const synchableProposals = this.calculateSynchableProposals([
                ...eventsByProposalId.keys(),
            ]);

            this.logger.info(
                `Reading events for the following proposals:\n${synchableProposals.join(", ")}`,
            );

            for (const proposalId of synchableProposals) {
                try {
                    const events = eventsByProposalId.get(proposalId) ?? [];

                    await this.syncProposal(proposalId, events, currentEpoch.number, lastBlock);
                } catch (err) {
                    await this.onActorError(proposalId, err as Error);
                }
            }

            this.logger.info(`Consumed events up to block ${lastBlock.number}.`);

            this.cleanupOldEpochs(currentEpoch.number);

            await this.createMissingProposals(currentEpoch);

            this.lastCheckedBlock = lastBlock.number;
        } catch (err) {
            if (isNativeError(err)) {
                this.logger.error(`Sync failed: ${err.message}\n\n${err.stack}`);
            } else {
                this.logger.error(`Sync failed: ${err}`);
            }

            await this.notifier.sendError("Error during synchronization", {}, err);
        }
    }

    /**
     * Fetches the current epoch for the protocol chain.
     *
     * @returns the current epoch properties of the protocol chain.
     */
    private async getCurrentEpoch(): Promise<Epoch> {
        this.logger.info("Fetching current epoch...");

        const currentEpoch = await this.protocolProvider.read.getCurrentEpoch();

        this.logger.info(`Current epoch fetched.`);
        this.logger.debug(stringify(currentEpoch));

        return currentEpoch;
    }

    /**
     * Fetches the last finalized block on the protocol chain.
     *
     * @returns the last finalized block
     */
    private async getLastFinalizedBlock(): Promise<Block<bigint, boolean, "finalized">> {
        this.logger.info("Fetching last finalized block...");

        const lastBlock = await this.protocolProvider.read.getLastFinalizedBlock();

        this.logger.info(`Last finalized block ${lastBlock.number} fetched.`);

        return lastBlock;
    }

    /**
     * Fetches the events to process during the sync.
     *
     * @param fromBlock block number lower bound for event search
     * @param toBlock block number upper bound for event search
     * @returns an array of events
     */
    private async getEvents(fromBlock: bigint, toBlock: bigint): Promise<EboEventV2[]> {
        this.logger.info(`Fetching events between blocks ${fromBlock} and ${toBlock}...`);

        const events = await this.protocolProvider.read.getEvents(fromBlock, toBlock);

        this.logger.info(`${events.length} events fetched.`);
        this.logger.debug(stringify(events));

        return events;
    }

    /**
     * Group events by its proposal ID.
     *
     * @param events a raw stream of events for, potentially, several proposals
     * @returns a map with proposal ID as a key and an array of the proposal's events as value.
     */
    private groupEventsByProposal(events: EboEventStream): Map<ProposalId, EboEventStream> {
        const groupedEvents = new Map<ProposalId, EboEventStream>();

        for (const event of events) {
            const proposalId = event.proposalId;
            const proposalEvents = groupedEvents.get(proposalId) || [];

            groupedEvents.set(proposalId, [...proposalEvents, event]);
        }

        return groupedEvents;
    }

    /**
     * Calculate the proposal IDs that should be considered for sync by merging the
     * proposal IDs read from events and the proposal IDs already being handled by an actor.
     *
     * @param eventsProposalIds proposal IDs observed in an events batch
     * @returns proposal IDs to sync
     */
    private calculateSynchableProposals(eventsProposalIds: ProposalId[]): ProposalId[] {
        const actorsProposalIds = this.actorsManager.getProposalIds();
        const uniqueProposalIds = new Set([...eventsProposalIds, ...actorsProposalIds]);

        return [...uniqueProposalIds];
    }

    /**
     * Sync the actor with new events and update the state based on the last block.
     *
     * @param proposalId the ID of the proposal
     * @param events a stream of consumed events
     * @param currentEpoch the current epoch based on the last block
     * @param lastBlock the last block checked
     */
    private async syncProposal(
        proposalId: ProposalId,
        events: EboEventStream,
        currentEpoch: Epoch["number"],
        lastBlock: Block<bigint, boolean, "finalized">,
    ) {
        const firstEvent = events[0];
        const actor = await this.getOrCreateActor(proposalId, firstEvent);

        if (!actor) {
            this.logger.warn(`Dropping unhandled events for proposal ${proposalId}`);
            return;
        }

        for (const event of events) {
            try {
                actor.enqueue(event);
            } catch (err) {
                if (err instanceof PastEventEnqueueError) {
                    this.logger.warn(
                        `Dropping already enqueued event at ${event.blockNumber} block ` +
                            `with log index ${event.logIndex}`,
                    );
                } else {
                    throw err;
                }
            }
        }

        await actor.processEvents();

        const lastBlockTimestamp = lastBlock.timestamp as UnixTimestamp;
        await actor.onLastBlockUpdated(lastBlockTimestamp);

        if (actor.canBeTerminated(currentEpoch)) {
            await this.terminateActor(proposalId);
        }
    }

    /**
     * Get the actor handling a specific proposal. If there's no actor created yet, it's created.
     *
     * @param proposalId the ID of the proposal the returned actor is handling
     * @param firstEvent an event to create an actor if it does not exist
     * @returns the actor handling the specified proposal
     */
    private async getOrCreateActor(proposalId: ProposalId, firstEvent?: EboEventV2) {
        const actor = this.actorsManager.getActor(proposalId);

        if (actor) return actor;

        if (firstEvent && isProposalCreatedEvent(firstEvent)) {
            const { epoch, chainId } = firstEvent.args;

            const caip2ChainId = chainId as Caip2ChainId;

            const isChainSupported = Caip2Utils.isSupported(caip2ChainId);

            if (isChainSupported) {
                this.logger.info(`Creating a new EboActor to handle proposal ${proposalId}...`);

                return this.createNewActor(proposalId, epoch, caip2ChainId);
            } else {
                this.logger.warn(`Chain ${caip2ChainId} not supported by the agent. Skipping...`);

                await this.notifier.sendError(
                    `Chain ${caip2ChainId} not supported by the agent. Skipping...`,
                    { chainId: caip2ChainId, proposalId },
                    new Error("Unsupported chain"),
                );

                return null;
            }
        } else {
            // If the first event is not a ProposalCreated,
            // the event was from another epoch OR the contracts are misbehaving
            return null;
        }
    }

    /**
     * Create a new actor based on a proposal.
     *
     * @param proposalId The ID of the proposal
     * @param epoch The epoch number
     * @param chainId The chain ID for the proposal
     * @returns A new EboActorV2 instance, or null if actor creation failed
     */
    private async createNewActor(proposalId: ProposalId, epoch: bigint, chainId: Caip2ChainId) {
        try {
            // Fetch the proposal from the protocol
            const proposal = await this.protocolProvider.read.getProposalById(proposalId);

            if (!proposal) {
                this.logger.error(`Could not find proposal ${proposalId}`);
                return null;
            }

            // Fetch the parameters using the snapshot ID from the proposal
            const parameters = await this.parametersRegistry.getParameters(
                proposal.parametersSnapshotId,
            );

            if (!parameters) {
                this.logger.error(
                    `Could not find parameters for snapshot ID ${proposal.parametersSnapshotId}`,
                );
                return null;
            }

            // Fetch the dispute information if the proposal is disputed or beyond
            // this shouldn't be needed
            let dispute = undefined;
            if (proposal.status >= ProposalStatus.Disputed) {
                dispute = await this.protocolProvider.read.getDisputeByProposalId(proposalId);
            }

            const actorProposal: ActorProposal = {
                id: proposalId,
                epoch,
                chainId,
                proposal,
                parameters,
                dispute,
            };

            const actor = this.actorsManager.createActor(
                actorProposal,
                this.protocolProvider,
                this.blockNumberService,
                this.logger,
                this.notifier,
            );

            this.addHandledChainId(epoch, chainId);

            return actor;
        } catch (error) {
            this.logger.error(`Failed to create actor for proposal ${proposalId}: ${error}`);
            return null;
        }
    }

    /**
     * Adds a handled chain ID to the map for a given epoch.
     *
     * @param epoch The epoch number.
     * @param chainId The chain ID.
     */
    private addHandledChainId(epoch: Epoch["number"], chainId: Caip2ChainId): void {
        if (!this.handledChainIdsPerEpoch.has(epoch)) {
            this.handledChainIdsPerEpoch.set(epoch, new Set());
        }
        this.handledChainIdsPerEpoch.get(epoch)!.add(chainId);
    }

    private async onActorError(proposalId: ProposalId, error: Error) {
        this.logger.error(
            `Critical error. Actor handling proposal ${proposalId} ` +
                `threw a non-recoverable error: ${error.message}\n\n` +
                `The proposal ${proposalId} will stop being tracked by the system.`,
        );

        await this.notifier.sendError(
            `Actor error for proposal ${proposalId}`,
            { proposalId },
            error,
        );

        await this.terminateActor(proposalId);
    }

    /**
     * Creates missing proposals for the specified epoch, based on the
     * available chains and the chains already handled.
     *
     * @param epoch the epoch number
     */
    private async createMissingProposals(epoch: Epoch): Promise<void> {
        try {
            this.logger.info("Fetching available chains...");

            const availableChains: Caip2ChainId[] =
                await this.protocolProvider.read.getAvailableChains();

            this.logger.info("Available chains fetched.");

            this.logger.info("Creating missing proposals...");

            for (const chain of availableChains) {
                if (
                    this.getCounterProposalsCreatedPerEpochAndChain(`${epoch.number}-${chain}`) <
                    this.maxProposalsPerEpochAndChain
                ) {
                    // Check if a proposal already exists for this epoch and chain
                    const existingProposal = await this.protocolProvider.read.getActiveProposal(
                        epoch.number,
                        chain,
                    );

                    if (!existingProposal) {
                        await this.createProposal(chain, epoch);
                        this.incrementCounterProposalsCreatedPerEpochAndChain(
                            `${epoch.number}-${chain}`,
                        );
                    }
                } else {
                    this.logger.info(
                        `Max proposals per epoch and chain reached for epoch ${epoch.number} and chain ${chain}. Skipping...`,
                    );
                }
            }

            this.logger.info("Missing proposals created.");
        } catch (err) {
            if (isNativeError(err)) {
                this.logger.error(`Proposals creation failed: ${err.message}`);
            } else {
                this.logger.error(`Proposals creation failed: ${err}`);
            }

            await this.notifier.sendError("Error creating missing proposals", { epoch }, err);
        }
    }

    private getCounterProposalsCreatedPerEpochAndChain(key: EpochChainKey): number {
        return this.counterProposalsCreatedPerEpochAndChain.get(key) ?? 0;
    }

    private incrementCounterProposalsCreatedPerEpochAndChain(key: EpochChainKey): void {
        if (!this.counterProposalsCreatedPerEpochAndChain.has(key)) {
            this.counterProposalsCreatedPerEpochAndChain.set(key, 0);
        }

        this.counterProposalsCreatedPerEpochAndChain.set(
            key,
            this.counterProposalsCreatedPerEpochAndChain.get(key)! + 1,
        );
    }

    /**
     * Creates a proposal for a given chain and epoch.
     *
     * @param chain The chain ID.
     * @param epoch The epoch number.
     */
    private async createProposal(chain: Caip2ChainId, epoch: Epoch) {
        this.logger.info(`Creating proposal for chain ${chain} and epoch ${epoch}...`);

        try {
            // Get the block number for this epoch and chain
            const blockNumber = await this.blockNumberService.getEpochBlockNumber(
                epoch.startTimestamp,
                chain,
            );

            // Create the proposal with the protocol provider
            const serviceProvider = this.protocolProvider.getAccountAddress();
            await this.protocolProvider.write.propose(
                epoch.number,
                chain,
                blockNumber,
                serviceProvider,
            );

            this.logger.info(`Proposal created for chain ${chain} and epoch ${epoch}`);
        } catch (err) {
            if (
                err instanceof CustomContractErrorV2 &&
                err.name === "EBOCore_Propose_ProposalAlreadyActive"
            ) {
                this.logger.info(
                    `Active proposal for epoch ${epoch} and chain ${chain} already created`,
                );
            } else {
                let errorMessage;
                if (err instanceof TransactionExecutionError) {
                    errorMessage = `Transaction reverted while creating proposal for chain ${chain} and epoch ${epoch}`;
                } else {
                    errorMessage = `Failed to create proposal for chain ${chain} and epoch ${epoch}: ${stringify(err)}`;
                }
                this.logger.error(errorMessage);
                await this.notifier.sendError(errorMessage, { chain, epoch }, err);
            }
        }
    }

    /**
     * Removes the actor from tracking the proposal.
     *
     * @param proposalId the ID of the proposal the actor is handling
     */
    private async terminateActor(proposalId: ProposalId) {
        this.logger.info(`Terminating actor handling proposal ${proposalId}...`);

        const deletedActor = this.actorsManager.deleteActor(proposalId);

        if (deletedActor) {
            this.logger.info(`Actor handling proposal ${proposalId} was terminated.`);
        } else {
            this.logger.warn(`Actor handling proposal ${proposalId} was already terminated.`);

            await this.notifier.sendError(
                `Actor handling proposal ${proposalId} was already terminated.`,
                { proposalId },
                new Error("Actor already deleted"),
            );
        }
    }

    /**
     * Cleans up old epochs from the handledChainIdsPerEpoch map.
     *
     * Note: When the agent supports handling old epochs, this cleanup strategy should be reviewed.
     *
     * @param currentEpoch The current epoch number.
     */
    private cleanupOldEpochs(currentEpoch: Epoch["number"]): void {
        // Drop data from epochs less than currentEpoch - 1
        for (const epoch of this.handledChainIdsPerEpoch.keys()) {
            if (epoch < currentEpoch - 1n) {
                const chainIds = this.handledChainIdsPerEpoch.get(epoch) || new Set();
                for (const chainId of chainIds) {
                    this.counterProposalsCreatedPerEpochAndChain.delete(`${epoch}-${chainId}`);
                }
                this.handledChainIdsPerEpoch.delete(epoch);
            }
        }
    }
}