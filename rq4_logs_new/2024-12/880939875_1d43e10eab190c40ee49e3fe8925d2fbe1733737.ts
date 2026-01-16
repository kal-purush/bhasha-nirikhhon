import { parentPort, workerData } from 'worker_threads';
import { DiscordAPIError } from 'discord.js';
import { getDiscordClient } from '../discord/client';
import { createProposalEmbed } from '../discord/utils/embeds';
import { getNotificationRecipients } from '../discord/utils/users';
import prisma from '@/lib/prisma';

interface WorkerData {
  proposalId: string;
}

interface DiscordErrorResponse {
  retry: boolean;
  log: boolean;
  delay?: number;
}

async function handleDiscordError(error: DiscordAPIError): Promise<DiscordErrorResponse> {
  switch (error.code) {
    case 50007: // Cannot send messages to this user
      return { retry: false, log: true };
    case 50016: // Rate limited
      return { 
        retry: true, 
        delay: 5000, // Default delay if retry_after is not available
        log: true
      };
    case 10013: // Unknown user
      return { retry: false, log: true };
    default:
      return { retry: true, delay: 5000, log: true };
  }
}

async function notifyDiscordUsers() {
  const { proposalId } = workerData as WorkerData;

  try {
    // Get proposal with all required relations
    const [proposal, proposalForNotification] = await Promise.all([
      // Get full proposal for notification recipients
      prisma.proposal.findUnique({
        where: { id: parseInt(proposalId) },
        include: {
          fundingRound: {
            include: {
              topic: true
            }
          }
        }
      }),
      // Get proposal with specific fields for embed
      prisma.proposal.findUnique({
        where: { id: parseInt(proposalId) },
        select: {
          id: true,
          proposalName: true, // This is the title field in our schema
          user: {
            select: {
              id: true,
              metadata: true
            }
          },
          fundingRound: {
            select: {
              id: true,
              name: true
            }
          }
        }
      })
    ]);

    if (!proposal?.fundingRound) {
      throw new Error(`Proposal ${proposalId} not found or has no funding round`);
    }

    // Get Discord client singleton
    const client = await getDiscordClient();

    // Get all recipient Discord IDs
    const recipientIds = await getNotificationRecipients({
      fundingRound: {
        topicId: proposal.fundingRound.topicId
      }
    });

    if (!proposalForNotification) {
      throw new Error(`Failed to get proposal data for notification`);
    }

    // Create embed once
    const embed = createProposalEmbed({
      id: proposalForNotification.id,
      title: proposalForNotification.proposalName,
      user: proposalForNotification.user,
      fundingRound: proposalForNotification.fundingRound
    });

    // Send notifications with rate limiting
    for (const discordId of recipientIds) {
      try {
        const user = await client.users.fetch(discordId);
        if (!user || user.bot) {
          console.error(`Invalid user ${discordId}`);
          continue;
        }

        await user.send({ embeds: [embed] });
        
        // Rate limit compliance - wait 500ms between messages
        await new Promise(resolve => setTimeout(resolve, 500));
      } catch (error) {
        if (error instanceof DiscordAPIError) {
          const { retry, delay, log } = await handleDiscordError(error);
          
          if (log) {
            console.error(`Discord API error for user ${discordId}:`, error);
          }

          if (retry && delay) {
            await new Promise(resolve => setTimeout(resolve, delay));
            continue;
          }
        } else {
          console.error(`Unexpected error for user ${discordId}:`, error);
        }
      }
    }

    // Signal completion to parent
    parentPort?.postMessage('completed');
  } catch (error) {
    console.error('Discord notification failed:', error);
    process.exit(1);
  }
}

notifyDiscordUsers(); 