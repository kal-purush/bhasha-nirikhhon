import { WorkflowRunEvent } from "@octokit/webhooks-types";
import { APIMessage } from "discord-api-types/v10";

export interface Env {
  DISCORD_WEBHOOK: string;
  DB: D1Database;
}

export interface DiscordState {
  MessageID: string;
  WorkflowRunID: string;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    if (request.method !== "POST") {
      return new Response(
        JSON.stringify({ error: "Only POST method is supported." }),
      );
    }

    let workflowEvent: WorkflowRunEvent | null = null;
    try {
      workflowEvent = await request.json();
    } catch (err) {
      return new Response(
        JSON.stringify({ error: "Malformed or missing JSON." }),
      );
    }

    // Check for an existing Discord message that we can update instead of posting a new one
    const workflowMessageState: DiscordState = await env.DB.prepare(
      "SELECT * FROM DiscordWebhook WHERE WorkflowRunID = ?",
    )
      .bind(workflowEvent?.workflow_run.id.toString())
      .first();

    if (workflowMessageState != undefined) {
      console.log(
        `Existing run found, updating message: ${workflowMessageState}`,
      );
      await sendWebhook(env, workflowEvent, workflowMessageState.MessageID);
    } else {
      console.log(
        "No `MessageID` stored for this workflow run, sending a new message.",
      );
      const webhookResponse = await sendWebhook(env, workflowEvent);

      await env.DB.prepare(
        "INSERT INTO DiscordWebhook (MessageID, WorkflowRunID) VALUES (?, ?)",
      )
        .bind(webhookResponse.id, workflowEvent?.workflow_run.id.toString())
        .run();
    }

    // If the workflow is in a completed state, remove the entry from the database
    if (workflowEvent?.action === "completed") {
      await env.DB.prepare("DELETE FROM DiscordWebhook WHERE WorkflowRunID = ?")
        .bind(workflowEvent?.workflow_run.id.toString())
        .run();
    }

    return Response.json({ success: true });
  },
};

async function sendWebhook(
  env: Env,
  workflowEvent: WorkflowRunEvent | null,
  messageId?: string,
) {
  /**
   * Sends or updates a Discord Webhook via the API.
   * @param env Worker environment variables
   * @param requestJson Workflow Run event from GitHub
   * @param messageId If specified, a `PATCH` request will be sent to update the existing `messageId`
   */
  let url = `${env.DISCORD_WEBHOOK}?wait=true`;

  if (messageId) {
    url = `${env.DISCORD_WEBHOOK}/messages/${messageId}?wait=true`;
  }

  // TODO: Support colours and info for workflow failures
  const colours = {
    requested: 0xffffff,
    in_progress: 0xe1d71a,
    completed: 0x23cb1d,
  };

  let embedColour = 0xffffff;

  if (workflowEvent?.action !== undefined) {
    embedColour = colours[workflowEvent.action];
  }

  const resp = await fetch(url, {
    method: messageId ? "PATCH" : "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      content: "",
      tts: false,
      embeds: [
        {
          type: "rich",
          title: `[${workflowEvent?.repository.full_name}] Workflow ${workflowEvent?.workflow_run.name} triggered by ${workflowEvent?.workflow_run.actor.login} is ${workflowEvent?.workflow_run.status} on branch ${workflowEvent?.workflow_run.head_branch}`,
          description: `**Trigger**: \`${workflowEvent?.workflow_run.event}\`\n**Status**: ${workflowEvent?.workflow_run.status}\n**Conclusion**: ${workflowEvent?.workflow_run.conclusion}\n**Commit**: ${workflowEvent?.workflow_run.head_sha}`,
          color: embedColour,
          author: {
            name: workflowEvent?.workflow_run.actor.login,
            url: workflowEvent?.workflow_run.actor.html_url,
            icon_url: workflowEvent?.workflow_run.actor.avatar_url,
          },
          url: workflowEvent?.workflow_run.html_url,
        },
      ],
    }),
  });

  const webhookResp: APIMessage = await resp.json();
  console.log(
    `Discord returned ${resp.status} ${
      resp.statusText
    } with body ${JSON.stringify(webhookResp)}`,
  );
  return webhookResp;
}