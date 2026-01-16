/* eslint-disable @typescript-eslint/no-explicit-any */

import NodeCache from 'node-cache';

const jiraCache = new NodeCache({ stdTTL: 300 }); // Cache responses for 5 minutes

const JIRA_BASE_URL = process.env.JIRA_BASE_URL;
const JIRA_EMAIL = process.env.JIRA_EMAIL;
const JIRA_TOKEN = process.env.JIRA_TOKEN;

if (!JIRA_BASE_URL || !JIRA_EMAIL || !JIRA_TOKEN) {
  console.error('Missing required environment variables for Jira configuration.');
}

interface JiraTicketResponse {
  key: string;
  status: string;
  assignee: string | null;
  created: string;
}

export async function getCachedJiraResponse(boardTicket: string): Promise<any> {
  const cached = jiraCache.get(boardTicket);
  if (cached) return cached;
  const response = await fetchJiraTicketStatus(boardTicket);
  if (response) jiraCache.set(boardTicket, response);
  return response;
}

export async function getValidBoardsFromJira(
  ticketsToCheck: Record<string, string[]>
): Promise<Record<string, string>> {
  try {
    const resolvedBoards: Record<string, string> = {};

    for (const [ticket, boardOptions] of Object.entries(ticketsToCheck)) {
      let selectedBoard: string | null = null;
      let latestCreatedDate: Date | null = null;

      const responses = await Promise.all(
        boardOptions.map(async (boardTicket) => {
          try {
            return await getCachedJiraResponse(boardTicket);
          } catch (error) {
            console.error(`Error for board ${boardTicket}:`, error);
            return null;
          }
        })
      );

      for (let i = 0; i < responses.length; i++) {
        const jiraResponse = responses[i];
        if (jiraResponse) {
          const { status, created } = jiraResponse;
          if (status) {
            const ticketCreatedDate = new Date(created);
            if (!latestCreatedDate || ticketCreatedDate > latestCreatedDate) {
              selectedBoard = boardOptions[i];
              latestCreatedDate = ticketCreatedDate;
            }
          }
        }
      }
      resolvedBoards[ticket] = selectedBoard ?? boardOptions[0];
    }
    return resolvedBoards;
  } catch (error) {
    console.error("Error in getValidBoardsFromJira:", error);
    throw error;
  }
}

export async function fetchJiraTicketStatus(ticketNumber: string): Promise<JiraTicketResponse | null> {
  const JIRA_API_URL = `${JIRA_BASE_URL}/rest/api/3/issue/${ticketNumber}`;
  try {
    const response = await fetch(JIRA_API_URL, {
      method: 'GET',
      headers: {
        'Authorization': `Basic ${Buffer.from(`${JIRA_EMAIL}:${JIRA_TOKEN}`).toString('base64')}`,
        'Accept': 'application/json'
      }
    });

    if (!response.ok) return null;

    const data = await response.json();

    const createdDate = new Date(data?.fields?.created ?? '').toLocaleString('en-US', {
      year: 'numeric',
      month: 'short',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      hour12: true,
    });

    return {
      key: data?.key || '',
      status: data?.fields?.status?.name || 'Unknown',
      assignee: data?.fields?.assignee?.displayName || null,
      created: createdDate
    };
  } catch (error) {
    console.error(`Error fetching Jira ticket for confirming the board ${ticketNumber}:`, error);
    return null;
  }
}