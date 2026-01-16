import { parentPort, workerData } from 'worker_threads';
import prisma from '@/lib/prisma';
import { OCVApiService } from '@/services/OCVApiService';
import { ProposalStatusMoveService } from '@/services/ProposalStatusMoveService';
import logger from '@/logging';
import { ProposalStatus, WorkerStatus } from '@prisma/client';
import { randomUUID } from 'crypto';
import { Prisma } from '@prisma/client';

// Types for metadata tracking
interface ProposalVoteInfo {
  ocv_eligible: boolean;
  reviewer_votes_given: number;
  reviewer_votes_required: number;
}

interface OCVWorkerMetadata {
  moved_from_consideration_to_deliberation: Record<string, ProposalVoteInfo>;
  moved_from_deliberation_to_consideration: Record<string, ProposalVoteInfo>;
  project_vote_status: Record<string, ProposalVoteInfo>;
}

interface CleanupWorkerMetadata {
  updated_jobs: Array<{
    id: string;
    name: string;
  }>;
}

const LOCK_KEY = 'ocv_vote_counting_job';

// Define job names as constants
const WORKER_JOBS = {
  OCV_VOTE_COUNTING: 'ocv-vote-counter',
  STALE_JOB_CLEANUP: 'ocv-worker-cleanup'
} as const;

// Extract lock management into reusable functions
async function isLockAcquired(): Promise<boolean> {
  const result = await prisma.$queryRaw<Array<{ pg_try_advisory_lock: boolean }>>`
    SELECT pg_try_advisory_lock(hashtext(${LOCK_KEY})) as pg_try_advisory_lock
  `;
  return result[0]?.pg_try_advisory_lock ?? false;
}

async function releaseLock(): Promise<void> {
  await prisma.$queryRaw`
    SELECT pg_advisory_unlock(hashtext(${LOCK_KEY}))
  `;
}

async function updateHeartbeat(
  id: string, 
  name: string, 
  status: WorkerStatus,
  metadata?: OCVWorkerMetadata | CleanupWorkerMetadata
) {
  // Convert metadata to a JSON-safe format
  const jsonMetadata: Prisma.NullableJsonNullValueInput = metadata ? JSON.parse(JSON.stringify(metadata)) : null;

  await prisma.workerHeartbeat.upsert({
    where: { id },
    create: { 
      id,
      name,
      status,
      metadata: jsonMetadata
    },
    update: { 
      lastHeartbeat: new Date(),
      status,
      metadata: jsonMetadata
    }
  });
}

async function cleanupStaleJobs(): Promise<void> {
  const cleanupId = randomUUID();
  
  try {
    const lockAcquired = await isLockAcquired();
    if (!lockAcquired) {
      return;
    }

    try {
      // Create cleanup job heartbeat
      await updateHeartbeat(cleanupId, WORKER_JOBS.STALE_JOB_CLEANUP, WorkerStatus.RUNNING);

      // Find all RUNNING jobs first, excluding the current cleanup job
      const runningJobs = await prisma.workerHeartbeat.findMany({
        where: { 
          status: WorkerStatus.RUNNING, 
          name: { in: [WORKER_JOBS.OCV_VOTE_COUNTING, WORKER_JOBS.STALE_JOB_CLEANUP] },
          id: { not: cleanupId } // Exclude current cleanup job
        },
        select: { id: true, name: true }
      });

      logger.info(`runningJobs: ${JSON.stringify(runningJobs)}`);

      if (runningJobs.length > 0) {
        // Update their status to FAILED
        await prisma.workerHeartbeat.updateMany({
          where: { 
            id: { in: runningJobs.map(job => job.id) }
          },
          data: { status: WorkerStatus.FAILED }
        });

        const metadata: CleanupWorkerMetadata = {
          updated_jobs: runningJobs.map(({ id, name }) => ({ id, name }))
        };

        await updateHeartbeat(
          cleanupId, 
          WORKER_JOBS.STALE_JOB_CLEANUP, 
          WorkerStatus.COMPLETED,
          metadata
        );

        logger.info(`[OCV Vote Counting] Cleaned up ${runningJobs.length} stale worker heartbeats`);
      } else {
        await updateHeartbeat(
          cleanupId, 
          WORKER_JOBS.STALE_JOB_CLEANUP, 
          WorkerStatus.COMPLETED,
          { updated_jobs: [] }
        );
      }
    } finally {
      await releaseLock();
    }
  } catch (error) {
    logger.error('[OCV Vote Counting] Error cleaning up stale jobs:', error);
    await updateHeartbeat(cleanupId, WORKER_JOBS.STALE_JOB_CLEANUP, WorkerStatus.FAILED);
  }
}

async function processProposals() {
  const jobId = randomUUID();
  const ocvService = new OCVApiService();
  const statusMoveService = new ProposalStatusMoveService(prisma);
  let heartbeatInterval: NodeJS.Timeout | undefined;

  const metadata: OCVWorkerMetadata = {
    moved_from_consideration_to_deliberation: {},
    moved_from_deliberation_to_consideration: {},
    project_vote_status: {}
  };

  try {
    const lockAcquired = await isLockAcquired();
    if (!lockAcquired) {
      logger.info('[OCV Vote Counting] Another job is running, skipping...', { jobId });
      return;
    }

    logger.info('[OCV Vote Counting] Starting vote counting job', { jobId });

    // Set up heartbeat
    await updateHeartbeat(jobId, WORKER_JOBS.OCV_VOTE_COUNTING, WorkerStatus.RUNNING);
    heartbeatInterval = setInterval(
      () => updateHeartbeat(jobId, WORKER_JOBS.OCV_VOTE_COUNTING, WorkerStatus.RUNNING), 
      30000
    );

    // Get active proposals
    const activeProposals = await prisma.proposal.findMany({
      where: {
        status: {
          in: [ProposalStatus.CONSIDERATION, ProposalStatus.DELIBERATION]
        }
      },
      include: {
        fundingRound: {
          include: {
            considerationPhase: true
          }
        }
      }
    });

    logger.info(`[OCV Vote Counting] Processing ${activeProposals.length} active proposals`, { jobId });

    for (const proposal of activeProposals) {
      try {
        if (!proposal.fundingRound?.considerationPhase) {
          logger.warn(`Proposal's ${proposal.id} funding round ${proposal.fundingRoundId} has no consideration phase`);
          continue;
        }

        const startTime = proposal.fundingRound.considerationPhase.startDate.getTime();
        const endTime = proposal.fundingRound.considerationPhase.endDate.getTime();

        // Fetch OCV votes
        const ocvData = await ocvService.getConsiderationVotes(proposal.id, startTime, endTime);
        const voteDataJson = JSON.parse(JSON.stringify(ocvData));

        // Update vote cache
        await prisma.oCVConsiderationVote.upsert({
          where: { proposalId: proposal.id },
          create: {
            proposalId: proposal.id,
            voteData: voteDataJson
          },
          update: {
            voteData: voteDataJson
          }
        });

        // Get previous status
        const previousStatus = proposal.status;

        // Check and move proposal status
        const moveResult = await statusMoveService.checkAndMoveProposal(proposal.id);
        
        if (moveResult) {
          const voteInfo: ProposalVoteInfo = {
            ocv_eligible: moveResult.ocvEligible,
            reviewer_votes_given: moveResult.reviewerVotesGiven,
            reviewer_votes_required: moveResult.reviewerVotesRequired
          };

          // Track status changes
          if (previousStatus === ProposalStatus.CONSIDERATION && 
              moveResult.newStatus === ProposalStatus.DELIBERATION) {
            metadata.moved_from_consideration_to_deliberation[proposal.id] = voteInfo;
          } else if (previousStatus === ProposalStatus.DELIBERATION && 
                     moveResult.newStatus === ProposalStatus.CONSIDERATION) {
            metadata.moved_from_deliberation_to_consideration[proposal.id] = voteInfo;
          }

          // Track overall status
          metadata.project_vote_status[proposal.id] = voteInfo;
        }

        logger.info(`[OCV Vote Counting] Successfully processed proposal ${proposal.id}`, { 
          jobId,
          proposalId: proposal.id 
        });

      } catch (error) {
        logger.error(`[OCV Vote Counting] Error processing proposal ${proposal.id}:`, error);
      }
    }

    await updateHeartbeat(
      jobId, 
      WORKER_JOBS.OCV_VOTE_COUNTING, 
      WorkerStatus.COMPLETED,
      metadata
    );
    
    logger.info('[OCV Vote Counting] Job completed successfully', { jobId });
    parentPort?.postMessage('completed');

  } catch (error) {
    logger.error('Vote counting job failed:', error);
    await updateHeartbeat(jobId, WORKER_JOBS.OCV_VOTE_COUNTING, WorkerStatus.FAILED);
    throw error;
  } finally {
    if (heartbeatInterval) clearInterval(heartbeatInterval);
    await releaseLock();
  }
}

async function main() {
  await cleanupStaleJobs();
  await processProposals();
}

main(); 