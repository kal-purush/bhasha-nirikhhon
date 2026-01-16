import emailService from "./emailService";
import {
  CreatePasswordEmailData,
  ResetPasswordEmailData,
  StatusChangeEmailData,
  TicketAssignedEmailData,
  TicketCreatedEmailData,
} from "./types";

type EmailJob = {
  id: string;
  type: string;
  to: string;
  data:
    | TicketCreatedEmailData
    | ResetPasswordEmailData
    | CreatePasswordEmailData
    | StatusChangeEmailData
    | TicketAssignedEmailData;
  attempts: number;
  createdAt: Date;
};

class EmailQueue {
  private queue: EmailJob[] = [];
  private processing = false;
  private maxRetries = 3;
  private batchSize = 5;

  async add(job: Omit<EmailJob, "id" | "attempts" | "createdAt">) {
    this.queue.push({
      id: crypto.randomUUID(),
      attempts: 0,
      createdAt: new Date(),
      ...job,
    });

    if (!this.processing) {
      this.process();
    }
  }

  private async process() {
    this.processing = true;

    while (this.queue.length > 0) {
      const batch = this.queue.splice(0, this.batchSize);

      await Promise.all(
        batch.map(async (job) => {
          try {
            await this.sendEmail(job);
            console.log(`Email sent successfully: ${job.id}`);
            // eslint-disable-next-line @typescript-eslint/no-unused-vars
          } catch (error) {
            if (job.attempts < this.maxRetries) {
              job.attempts++;
              this.queue.push(job);
              console.error(`Retry ${job.attempts} for email ${job.id}`);
            } else {
              console.error(
                `Failed to send email ${job.id} after ${this.maxRetries} attempts`
              );
            }
          }
        })
      );

      await new Promise((resolve) => setTimeout(resolve, 1000));
    }

    this.processing = false;
  }

  private async sendEmail(job: EmailJob) {
    switch (job.type) {
      case "ticket-created":
        await emailService.sendTicketCreatedEmail(
          job.to,
          job.data as TicketCreatedEmailData
        );
        break;
      case "reset-password":
        await emailService.sendResetPasswordEmail(
          job.to,
          job.data as ResetPasswordEmailData
        );
        break;
      case "create-password":
        await emailService.sendCreatePasswordEmail(
          job.to,
          job.data as CreatePasswordEmailData
        );
        break;
      case "status-change":
        await emailService.sendStatusChangeEmail(
          job.to,
          job.data as StatusChangeEmailData
        );
        break;
      case "ticket-assigned":
        await emailService.sendTicketAssignedEmail(
          job.to,
          job.data as TicketAssignedEmailData
        );
        break;
    }
  }
}

export const emailQueue = new EmailQueue();